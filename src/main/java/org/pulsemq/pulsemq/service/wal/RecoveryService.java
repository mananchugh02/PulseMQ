package org.pulsemq.pulsemq.service.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class RecoveryService {

    private final ObjectMapper objectMapper;
    private final WalProperties walProperties;
    private final QueueRepository queueRepository;
    private final MessageRepository messageRepository;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;
    private final BrokerRecoveryState brokerRecoveryState;

    @EventListener(ApplicationStartedEvent.class)
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public void onApplicationStarted() {
        recoverFromWal();
    }

    public synchronized void recoverFromWal() {
        if (!walProperties.isEnabled()) {
            brokerRecoveryState.markRecovered();
            return;
        }

        try {
            Map<UUID, RecoveryEntry> recovered = new LinkedHashMap<>();
            for (Path walFile : resolveWalFiles()) {
                replayWalFile(walFile, recovered);
            }
            restoreMissingRuntimeStateFromDatabase(recovered);
            log.info("Recovered {} WAL message state(s) into runtime queues", recovered.size());
            brokerRecoveryState.markRecovered();
        } catch (Exception e) {
            log.error("Failed to recover PulseMQ runtime state from WAL", e);
            brokerRecoveryState.reset();
            throw new IllegalStateException("Failed to recover runtime state from WAL", e);
        }
    }

    private List<Path> resolveWalFiles() throws IOException {
        Path currentFile = Path.of(walProperties.getDirectory(), walProperties.getFileName());
        Path archiveDirectory = Path.of(walProperties.getArchiveDirectory());

        List<Path> walFiles = new ArrayList<>();
        if (Files.exists(archiveDirectory)) {
            try (var paths = Files.list(archiveDirectory)) {
                paths.filter(Files::isRegularFile).forEach(walFiles::add);
            }
        }

        if (Files.exists(currentFile)) {
            walFiles.add(currentFile);
        }

        walFiles.sort(Comparator
                .comparing((Path path) -> {
                    try {
                        return Files.getLastModifiedTime(path).toInstant();
                    } catch (IOException e) {
                        return Instant.EPOCH;
                    }
                })
                .thenComparing(Path::toString));
        return walFiles;
    }

    private void replayWalFile(Path walFile, Map<UUID, RecoveryEntry> recovered) {
        try (var lines = Files.lines(walFile)) {
            lines.map(String::trim)
                    .filter(StringUtils::hasText)
                    .forEach(line -> {
                        try {
                            WalEvent event = objectMapper.readValue(line, WalEvent.class);
                            applyEvent(event, recovered);
                        } catch (Exception parseError) {
                            log.warn("Skipping malformed WAL line in {}: {}", walFile, line, parseError);
                        }
                    });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read WAL file: " + walFile, e);
        }
    }

    private void applyEvent(WalEvent event, Map<UUID, RecoveryEntry> recovered) {
        if (event == null || event.getEventType() == null || event.getMessageId() == null) {
            return;
        }

        RecoveryEntry entry = recovered.computeIfAbsent(event.getMessageId(), id -> new RecoveryEntry(event.getMessageId()));
        entry.updateMetadata(event);

        switch (event.getEventType()) {
            case PUBLISH -> restorePublishedMessage(entry, event, recovered);
            case CLAIM -> restoreClaimedMessage(entry, event);
            case ACK -> markTerminal(entry, RuntimeState.ACKED);
            case NACK -> markPendingRetry(entry);
            case RETRY -> restoreRetry(entry, event);
            case DLQ_MOVE -> restoreDlqMove(entry, event);
            case TIMEOUT_REQUEUE -> restoreTimeoutRequeue(entry, event);
        }
    }

    private void restorePublishedMessage(RecoveryEntry entry, WalEvent event, Map<UUID, RecoveryEntry> recovered) {
        if (entry.state == RuntimeState.READY || entry.state == RuntimeState.IN_FLIGHT) {
            return;
        }

        QueueEntity queue = resolveQueue(event.getQueueId(), event.getQueueName(), event.getQueueType());
        entry.sourceQueue = queue;
        entry.templateMessage = buildTemplateMessage(event, queue, event.getTimestamp(), null);
        entry.state = RuntimeState.READY;
        inMemoryQueueRegistry.registerQueue(queue).restoreReadyMessage(entry.templateMessage);
    }

    private void restoreClaimedMessage(RecoveryEntry entry, WalEvent event) {
        QueueEntity queue = resolveQueue(event.getQueueId(), event.getQueueName(), event.getQueueType());
        entry.sourceQueue = queue;
        if (entry.templateMessage == null) {
            entry.templateMessage = buildTemplateMessage(event, queue, event.getTimestamp(), event.getTimestamp());
        }
        entry.state = RuntimeState.IN_FLIGHT;
        inMemoryQueueRegistry.registerQueue(queue).restoreInflightMessage(buildInflightMessage(entry, queue, event.getTimestamp()));
    }

    private void markTerminal(RecoveryEntry entry, RuntimeState state) {
        if (state == RuntimeState.READY || state == RuntimeState.IN_FLIGHT || state == RuntimeState.PENDING_RETRY) {
            return;
        }
        entry.state = state;
    }

    private void markPendingRetry(RecoveryEntry entry) {
        entry.state = RuntimeState.PENDING_RETRY;
        if (entry.sourceQueue != null && entry.templateMessage != null) {
            inMemoryQueueRegistry.getQueue(entry.sourceQueue.getId()).ifPresent(queue -> queue.removeReadyMessage(entry.templateMessage.getMessageId()));
            inMemoryQueueRegistry.getQueue(entry.sourceQueue.getId()).ifPresent(queue -> queue.completeProcessing(entry.templateMessage.getMessageId()));
        }
    }

    private void restoreRetry(RecoveryEntry entry, WalEvent event) {
        QueueEntity queue = resolveQueue(event.getQueueId(), event.getQueueName(), event.getQueueType());
        entry.sourceQueue = queue;
        if (entry.templateMessage == null) {
            entry.templateMessage = buildTemplateMessage(event, queue, event.getTimestamp(), null);
        }
        entry.state = RuntimeState.READY;
        inMemoryQueueRegistry.registerQueue(queue).restoreReadyMessage(buildReadyMessage(entry, queue, event.getTimestamp()));
    }

    private void restoreDlqMove(RecoveryEntry entry, WalEvent event) {
        QueueEntity sourceQueue = resolveQueue(event.getQueueId(), event.getQueueName(), event.getQueueType());
        QueueEntity targetQueue = event.getTargetQueueId() != null || StringUtils.hasText(event.getTargetQueueName())
                ? resolveQueue(event.getTargetQueueId(), event.getTargetQueueName(), event.getTargetQueueType())
                : sourceQueue;
        entry.sourceQueue = sourceQueue;
        entry.targetQueue = targetQueue;
        if (entry.templateMessage == null) {
            entry.templateMessage = buildTemplateMessage(event, sourceQueue, event.getTimestamp(), null);
        }
        entry.state = RuntimeState.DLQ;
        inMemoryQueueRegistry.registerQueue(targetQueue).restoreReadyMessage(buildReadyMessage(entry, targetQueue, event.getTimestamp()));
    }

    private void restoreTimeoutRequeue(RecoveryEntry entry, WalEvent event) {
        QueueEntity queue = resolveQueue(event.getQueueId(), event.getQueueName(), event.getQueueType());
        entry.sourceQueue = queue;
        if (entry.templateMessage == null) {
            entry.templateMessage = buildTemplateMessage(event, queue, event.getTimestamp(), null);
        }
        entry.state = RuntimeState.READY;
        inMemoryQueueRegistry.registerQueue(queue).restoreReadyMessage(buildReadyMessage(entry, queue, event.getTimestamp()));
    }

    protected void restoreMissingRuntimeStateFromDatabase(Map<UUID, RecoveryEntry> recovered) {
        restoreMissingReadyMessages(recovered);
        restoreMissingInflightMessages(recovered);
        restoreMissingDlqMessages(recovered);
    }

    private void restoreMissingReadyMessages(Map<UUID, RecoveryEntry> recovered) {
        for (MessageEntity message : messageRepository.findAllByStatus(MessageStatus.READY)) {
            if (message == null || message.getId() == null || recovered.containsKey(message.getId())) {
                continue;
            }

            QueueEntity queue = resolveQueueFromEntity(message.getQueue());
            RecoveryEntry entry = recovered.computeIfAbsent(message.getId(), RecoveryEntry::new);
            entry.sourceQueue = queue;
            entry.templateMessage = buildTemplateMessageFromEntity(message, queue, null);
            entry.state = RuntimeState.READY;
            inMemoryQueueRegistry.registerQueue(queue).restoreReadyMessage(entry.templateMessage);
        }
    }

    private void restoreMissingInflightMessages(Map<UUID, RecoveryEntry> recovered) {
        for (MessageEntity message : messageRepository.findAllByStatus(MessageStatus.IN_FLIGHT)) {
            if (message == null || message.getId() == null || recovered.containsKey(message.getId())) {
                continue;
            }

            QueueEntity queue = resolveQueueFromEntity(message.getQueue());
            RecoveryEntry entry = recovered.computeIfAbsent(message.getId(), RecoveryEntry::new);
            entry.sourceQueue = queue;
            entry.templateMessage = buildTemplateMessageFromEntity(message, queue, message.getUpdatedAt());
            entry.state = RuntimeState.IN_FLIGHT;
            inMemoryQueueRegistry.registerQueue(queue).restoreInflightMessage(buildInflightMessage(entry, queue, message.getUpdatedAt()));
        }
    }

    private void restoreMissingDlqMessages(Map<UUID, RecoveryEntry> recovered) {
        for (MessageEntity message : messageRepository.findAllByStatus(MessageStatus.DLQ)) {
            if (message == null || message.getId() == null || recovered.containsKey(message.getId())) {
                continue;
            }

            QueueEntity queue = resolveQueueFromEntity(message.getQueue());
            RecoveryEntry entry = recovered.computeIfAbsent(message.getId(), RecoveryEntry::new);
            entry.targetQueue = queue;
            entry.templateMessage = buildTemplateMessageFromEntity(message, queue, message.getUpdatedAt());
            entry.state = RuntimeState.DLQ;
            inMemoryQueueRegistry.registerQueue(queue).restoreReadyMessage(entry.templateMessage);
        }
    }

    private QueueEntity resolveQueue(UUID queueId, String queueName, QueueType queueType) {
        if (queueId != null) {
            Optional<QueueEntity> queueEntity = queueRepository.getQueueById(queueId);
            if (queueEntity.isPresent()) {
                return queueEntity.get();
            }
        }

        if (StringUtils.hasText(queueName)) {
            Optional<QueueEntity> queueEntity = queueRepository.findByName(queueName);
            if (queueEntity.isPresent()) {
                return queueEntity.get();
            }
        }

        return QueueEntity.builder()
                .id(queueId)
                .name(StringUtils.hasText(queueName) ? queueName : Objects.toString(queueId, "wal-queue"))
                .type(queueType != null ? queueType : QueueType.MAIN)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    private QueueEntity resolveQueueFromEntity(QueueEntity queueEntity) {
        UUID queueId = queueEntity != null ? queueEntity.getId() : null;
        if (queueId != null) {
            Optional<QueueEntity> fromDatabase = queueRepository.getQueueById(queueId);
            if (fromDatabase.isPresent()) {
                return fromDatabase.get();
            }
        }

        return QueueEntity.builder()
                .id(queueId)
                .name(queueId != null ? queueId.toString() : "wal-queue")
                .type(QueueType.MAIN)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    private QueuedMessage buildTemplateMessage(WalEvent event, QueueEntity queueEntity, Instant enqueuedAt, Instant inflightAt) {
        return QueuedMessage.builder()
                .messageId(event.getMessageId())
                .queueId(queueEntity != null ? queueEntity.getId() : event.getQueueId())
                .routingKey(event.getRoutingKey())
                .payload(event.getPayload())
                .headers(event.getHeaders() != null ? Map.copyOf(event.getHeaders()) : Map.of())
                .enqueuedAt(enqueuedAt)
                .inflightAt(inflightAt)
                .build();
    }

    private QueuedMessage buildTemplateMessageFromEntity(MessageEntity message, QueueEntity queueEntity, Instant inflightAt) {
        return QueuedMessage.builder()
                .messageId(message.getId())
                .queueId(queueEntity != null ? queueEntity.getId() : message.getQueue() != null ? message.getQueue().getId() : null)
                .routingKey(null)
                .payload(message.getPayload())
                .headers(Map.of())
                .enqueuedAt(message.getVisibleAt() != null ? message.getVisibleAt() : Instant.now())
                .inflightAt(inflightAt)
                .build();
    }

    private QueuedMessage buildReadyMessage(RecoveryEntry entry, QueueEntity queue, Instant enqueuedAt) {
        return QueuedMessage.builder()
                .messageId(entry.messageId)
                .queueId(queue.getId())
                .routingKey(entry.templateMessage != null ? entry.templateMessage.getRoutingKey() : null)
                .payload(entry.templateMessage != null ? entry.templateMessage.getPayload() : null)
                .headers(entry.templateMessage != null && entry.templateMessage.getHeaders() != null ? Map.copyOf(entry.templateMessage.getHeaders()) : Map.of())
                .enqueuedAt(enqueuedAt)
                .build();
    }

    private QueuedMessage buildInflightMessage(RecoveryEntry entry, QueueEntity queue, Instant inflightAt) {
        return QueuedMessage.builder()
                .messageId(entry.messageId)
                .queueId(queue.getId())
                .routingKey(entry.templateMessage != null ? entry.templateMessage.getRoutingKey() : null)
                .payload(entry.templateMessage != null ? entry.templateMessage.getPayload() : null)
                .headers(entry.templateMessage != null && entry.templateMessage.getHeaders() != null ? Map.copyOf(entry.templateMessage.getHeaders()) : Map.of())
                .enqueuedAt(entry.templateMessage != null ? entry.templateMessage.getEnqueuedAt() : inflightAt)
                .inflightAt(inflightAt)
                .build();
    }

    private enum RuntimeState {
        READY,
        IN_FLIGHT,
        PENDING_RETRY,
        ACKED,
        DLQ
    }

    private static class RecoveryEntry {
        private final UUID messageId;
        private WalEvent lastEvent;
        private QueuedMessage templateMessage;
        private QueueEntity sourceQueue;
        private QueueEntity targetQueue;
        private RuntimeState state;

        private RecoveryEntry(UUID messageId) {
            this.messageId = messageId;
        }

        private void updateMetadata(WalEvent event) {
            this.lastEvent = event;
            if (templateMessage == null && event.getPayload() != null) {
                this.templateMessage = QueuedMessage.builder()
                        .messageId(event.getMessageId())
                        .queueId(event.getQueueId())
                        .routingKey(event.getRoutingKey())
                        .payload(event.getPayload())
                        .headers(event.getHeaders() != null ? Map.copyOf(event.getHeaders()) : Map.of())
                        .enqueuedAt(event.getTimestamp())
                        .build();
            }
        }
    }
}



