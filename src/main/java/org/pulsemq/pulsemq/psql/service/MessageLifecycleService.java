package org.pulsemq.pulsemq.psql.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.ResponseDTO.MessageLifecycleResponseDTO;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.pulsemq.pulsemq.service.wal.WalEvent;
import org.pulsemq.pulsemq.service.wal.WalEventRecorder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageLifecycleService {

    private static final int MAX_RETRY_COUNT = 3;

    private final MessageRepository messageRepository;
    private final QueueRepository queueRepository;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;
    private final org.pulsemq.pulsemq.service.QueueMetricsService queueMetricsService;
    private final WalEventRecorder walEventRecorder;

    @Transactional
    public MessageLifecycleResponseDTO ackMessage(UUID queueId, UUID messageId) {
        return processMessage(queueId, messageId, LifecycleAction.ACK);
    }

    @Transactional
    public MessageLifecycleResponseDTO nackMessage(UUID queueId, UUID messageId) {
        return processMessage(queueId, messageId, LifecycleAction.NACK);
    }

    private MessageLifecycleResponseDTO processMessage(UUID queueId, UUID messageId, LifecycleAction lifecycleAction) {
        validateIdentifiers(queueId, messageId);

        QueueEntity queueEntity = queueRepository.getQueueById(queueId)
                .orElseThrow(() -> new EntityNotFoundException("Queue not found for id: " + queueId));

        MessageEntity messageEntity = messageRepository.findByIdAndQueue_Id(messageId, queueId)
                .orElseThrow(() -> new EntityNotFoundException("Message not found for id: " + messageId + " in queue: " + queueId));

        if (isTerminalStatus(messageEntity.getStatus())) {
            throw new IllegalStateException("Message " + messageId + " is already in terminal state " + messageEntity.getStatus());
        }

        if (messageEntity.getStatus() != MessageStatus.IN_FLIGHT) {
            throw new IllegalStateException("Message " + messageId + " is not in IN_FLIGHT state for " + lifecycleAction + " (current: " + messageEntity.getStatus() + ")");
        }

        InMemoryQueue inMemoryQueue = inMemoryQueueRegistry.getQueue(queueId)
                .orElseThrow(() -> new EntityNotFoundException("Runtime queue not found for id: " + queueId));

        QueuedMessage runtimeMessage = java.util.Optional.ofNullable(inMemoryQueue.getInFlightMessages().get(messageId))
                .orElseThrow(() -> new IllegalStateException("Message " + messageId + " is not present in runtime in-flight tracking for queue " + queueId));

        Instant now = Instant.now();
        if (lifecycleAction == LifecycleAction.ACK) {
            inMemoryQueue.completeProcessing(messageId);
            messageEntity.setStatus(MessageStatus.ACKED);
            messageEntity.setVisibleAt(now);
            MessageEntity saved = messageRepository.save(messageEntity);
            log.info("Acknowledged message {} in queue {}", messageId, queueId);
            walEventRecorder.record(WalEvent.ack(runtimeMessage, queueEntity, now));
            queueMetricsService.incrementAck(queueId.toString());
            queueMetricsService.incrementConsumed(queueId.toString());
            return buildResponse(queueEntity, saved, lifecycleAction.name(), false, inMemoryQueue, now);
        }

        inMemoryQueue.completeProcessing(messageId);

        // mark consumed and nack metrics
        queueMetricsService.incrementConsumed(queueId.toString());
        queueMetricsService.incrementNack(queueId.toString());
        walEventRecorder.record(WalEvent.nack(runtimeMessage, queueEntity, safeRetryCount(messageEntity.getRetryCount()) + 1, now));

        int nextRetryCount = safeRetryCount(messageEntity.getRetryCount()) + 1;
        boolean deadLettered = nextRetryCount > MAX_RETRY_COUNT;
        messageEntity.setRetryCount(nextRetryCount);

        if (deadLettered) {
            // move to DLQ immediately
            // set original queue id before moving to DLQ
            messageEntity.setOriginalQueueId(queueEntity.getId());

            QueueEntity dlqEntity = queueEntity.getDeadLetterQueue();
            if (dlqEntity == null) {
                log.warn("No DLQ associated with queue {} - cannot move message {} to DLQ", queueId, messageId);
                // fallback: mark as DLQ status but do not change queue reference
                messageEntity.setVisibleAt(now);
                messageEntity.setStatus(MessageStatus.DLQ);
                MessageEntity saved = messageRepository.save(messageEntity);
                walEventRecorder.record(WalEvent.dlqMove(runtimeMessage, queueEntity, null, nextRetryCount, now));
                return buildResponse(queueEntity, saved, lifecycleAction.name(), true, inMemoryQueue, now);
            }

            // update message to reference DLQ queue
            messageEntity.setQueue(dlqEntity);
            messageEntity.setVisibleAt(now);
            messageEntity.setStatus(MessageStatus.DLQ);

            QueuedMessage requeuedMessage = QueuedMessage.builder()
                    .messageId(runtimeMessage.getMessageId())
                    .queueId(dlqEntity.getId())
                    .routingKey(runtimeMessage.getRoutingKey())
                    .payload(runtimeMessage.getPayload())
                    .headers(runtimeMessage.getHeaders())
                    .enqueuedAt(now)
                    .build();

            InMemoryQueue dlqRuntime = inMemoryQueueRegistry.getQueue(dlqEntity.getId())
                    .orElseGet(() -> inMemoryQueueRegistry.registerQueue(dlqEntity));
            dlqRuntime.enqueue(requeuedMessage);
            log.warn("Message {} moved to DLQ {} (for source queue {}) after {} retries", messageId, dlqEntity.getId(), queueId, nextRetryCount);
            walEventRecorder.record(WalEvent.dlqMove(runtimeMessage, queueEntity, dlqEntity, nextRetryCount, now));
            queueMetricsService.incrementDlq(dlqEntity.getId().toString());

            MessageEntity saved = messageRepository.save(messageEntity);
            return buildResponse(queueEntity, saved, lifecycleAction.name(), true, inMemoryQueue, now);
        }

        // schedule delayed retry using visibleAt and RETRY_PENDING status
        java.time.Duration delay = computeRetryDelaySeconds(nextRetryCount);
        Instant nextVisible = now.plus(delay);
        messageEntity.setVisibleAt(nextVisible);
        messageEntity.setStatus(MessageStatus.RETRY_PENDING);

        MessageEntity saved = messageRepository.save(messageEntity);
        log.info("Scheduled retry for message {} in queue {} retryCount={} visibleAt={} (delay {}s)",
                messageId, queueId, nextRetryCount, nextVisible, delay.getSeconds());
        queueMetricsService.incrementRetry(queueId.toString());

        return buildResponse(queueEntity, saved, lifecycleAction.name(), false, inMemoryQueue, now);
    }

    private void validateIdentifiers(UUID queueId, UUID messageId) {
        if (queueId == null) {
            throw new IllegalArgumentException("Queue id must not be null");
        }
        if (messageId == null) {
            throw new IllegalArgumentException("Message id must not be null");
        }
    }

    private int safeRetryCount(Integer retryCount) {
        return retryCount == null ? 0 : retryCount;
    }

    private java.time.Duration computeRetryDelaySeconds(int retryCount) {
        return switch (retryCount) {
            case 1 -> java.time.Duration.ofSeconds(5);
            case 2 -> java.time.Duration.ofSeconds(30);
            case 3 -> java.time.Duration.ofMinutes(2);
            default -> java.time.Duration.ofMinutes(5);
        };
    }

    private boolean isTerminalStatus(MessageStatus status) {
        return status == MessageStatus.ACKED || status == MessageStatus.DLQ || status == MessageStatus.PURGED;
    }

    private MessageLifecycleResponseDTO buildResponse(QueueEntity queueEntity,
                                                      MessageEntity messageEntity,
                                                      String action,
                                                      boolean deadLettered,
                                                      InMemoryQueue inMemoryQueue,
                                                      Instant processedAt) {
        int dlqSize = 0;
        if (queueEntity != null && queueEntity.getDeadLetterQueue() != null) {
            dlqSize = inMemoryQueueRegistry.getQueue(queueEntity.getDeadLetterQueue().getId())
                    .map(InMemoryQueue::size)
                    .orElse(0);
        }

        return MessageLifecycleResponseDTO.builder()
                .queueId(queueEntity.getId())
                .queueName(queueEntity.getName())
                .messageId(messageEntity.getId())
                .action(action)
                .status(messageEntity.getStatus())
                .retryCount(messageEntity.getRetryCount())
                .deadLettered(deadLettered)
                .readyQueueSize(inMemoryQueue.size())
                .deadLetterQueueSize(dlqSize)
                .processedAt(processedAt)
                .build();
    }

    private enum LifecycleAction {
        ACK,
        NACK
    }
}


