package org.pulsemq.pulsemq.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.service.wal.WalEvent;
import org.pulsemq.pulsemq.service.wal.WalEventRecorder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class VisibilityTimeoutService {

    private static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    private final InMemoryQueueRegistry inMemoryQueueRegistry;
    private final MessageRepository messageRepository;
    private final org.pulsemq.pulsemq.service.QueueMetricsService queueMetricsService;
    private final WalEventRecorder walEventRecorder;

    /**
     * Scan runtime queues and recover messages that exceeded the visibility timeout.
     * This will NOT increment retry counts; timeout recovery is not considered a failure.
     */
    @Transactional
    public void recoverTimedOutMessages() {
        Instant now = Instant.now();

        for (InMemoryQueue queue : inMemoryQueueRegistry.getAllQueues()) {
            if (queue == null) continue;
            // iterate over a snapshot of in-flight messages
            for (Map.Entry<UUID, QueuedMessage> entry : queue.getInFlightMessages().entrySet()) {
                UUID messageId = entry.getKey();
                QueuedMessage runtimeMessage = entry.getValue();
                if (runtimeMessage == null) continue;
                Instant inflightAt = runtimeMessage.getInflightAt();
                if (inflightAt == null) {
                    continue;
                }

                Duration elapsed = Duration.between(inflightAt, now);
                if (elapsed.compareTo(VISIBILITY_TIMEOUT) <= 0) {
                    continue;
                }

                // timed out: remove from inflight tracking and requeue
                Optional<QueuedMessage> removed = queue.completeProcessing(messageId);
                if (removed.isEmpty()) {
                    continue;
                }

                // create new ready runtime message and enqueue
                QueuedMessage requeued = QueuedMessage.builder()
                        .messageId(runtimeMessage.getMessageId())
                        .queueId(runtimeMessage.getQueueId())
                        .routingKey(runtimeMessage.getRoutingKey())
                        .payload(runtimeMessage.getPayload())
                        .headers(runtimeMessage.getHeaders())
                        .enqueuedAt(now)
                        .build();

                boolean offered = queue.enqueue(requeued);

                // persist status back to READY and update visibleAt
                Optional<MessageEntity> maybe = messageRepository.getMessageById(messageId);
                if (maybe.isPresent()) {
                    MessageEntity messageEntity = maybe.get();
                    messageEntity.setStatus(MessageStatus.READY);
                    messageEntity.setVisibleAt(now);
                    messageRepository.save(messageEntity);
                }

                walEventRecorder.record(WalEvent.timeoutRequeue(requeued, toQueueEntity(queue), now));

                log.info("Recovered timed out message {} for queue {} - requeued: {}",
                        messageId, queue.getQueueName(), offered);
                // metrics: recovery is not a failure; requeue considered a publish
                queueMetricsService.incrementPublished(queue.getQueueId().toString());
            }
        }
    }

    private QueueEntity toQueueEntity(InMemoryQueue queue) {
        return QueueEntity.builder()
                .id(queue.getQueueId())
                .name(queue.getQueueName())
                .type(queue.getQueueType())
                .createdAt(queue.getCreatedAt())
                .updatedAt(queue.getUpdatedAt())
                .build();
    }
}

