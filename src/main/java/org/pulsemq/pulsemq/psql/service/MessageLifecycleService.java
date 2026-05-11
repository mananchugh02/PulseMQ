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

        InMemoryQueue inMemoryQueue = inMemoryQueueRegistry.getQueue(queueId)
                .orElseThrow(() -> new EntityNotFoundException("Runtime queue not found for id: " + queueId));

        QueuedMessage runtimeMessage = inMemoryQueue.claimMessage(messageId)
                .orElseThrow(() -> new IllegalStateException("Message " + messageId + " is not present in runtime tracking for queue " + queueId));

        Instant now = Instant.now();
        if (lifecycleAction == LifecycleAction.ACK) {
            inMemoryQueue.completeProcessing(messageId);
            messageEntity.setStatus(MessageStatus.ACKED);
            messageEntity.setVisibleAt(now);
            MessageEntity saved = messageRepository.save(messageEntity);
            log.info("Acknowledged message {} in queue {}", messageId, queueId);
            return buildResponse(queueEntity, saved, lifecycleAction.name(), false, inMemoryQueue, now);
        }

        inMemoryQueue.completeProcessing(messageId);

        int nextRetryCount = safeRetryCount(messageEntity.getRetryCount()) + 1;
        boolean deadLettered = nextRetryCount > MAX_RETRY_COUNT;
        messageEntity.setRetryCount(nextRetryCount);
        messageEntity.setVisibleAt(now);
        messageEntity.setStatus(deadLettered ? MessageStatus.DLQ : MessageStatus.READY);

        QueuedMessage requeuedMessage = QueuedMessage.builder()
                .messageId(runtimeMessage.getMessageId())
                .queueId(runtimeMessage.getQueueId())
                .routingKey(runtimeMessage.getRoutingKey())
                .payload(runtimeMessage.getPayload())
                .headers(runtimeMessage.getHeaders())
                .enqueuedAt(now)
                .build();

        if (deadLettered) {
            inMemoryQueue.enqueueDeadLetter(requeuedMessage);
            log.warn("Message {} moved to DLQ for queue {} after {} retries", messageId, queueId, nextRetryCount);
        } else {
            inMemoryQueue.enqueue(requeuedMessage);
            log.info("Message {} requeued for queue {} with retry count {}", messageId, queueId, nextRetryCount);
        }

        MessageEntity saved = messageRepository.save(messageEntity);
        return buildResponse(queueEntity, saved, lifecycleAction.name(), deadLettered, inMemoryQueue, now);
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

    private boolean isTerminalStatus(MessageStatus status) {
        return status == MessageStatus.ACKED || status == MessageStatus.DLQ || status == MessageStatus.PURGED;
    }

    private MessageLifecycleResponseDTO buildResponse(QueueEntity queueEntity,
                                                      MessageEntity messageEntity,
                                                      String action,
                                                      boolean deadLettered,
                                                      InMemoryQueue inMemoryQueue,
                                                      Instant processedAt) {
        return MessageLifecycleResponseDTO.builder()
                .queueId(queueEntity.getId())
                .queueName(queueEntity.getName())
                .messageId(messageEntity.getId())
                .action(action)
                .status(messageEntity.getStatus())
                .retryCount(messageEntity.getRetryCount())
                .deadLettered(deadLettered)
                .readyQueueSize(inMemoryQueue.size())
                .deadLetterQueueSize(inMemoryQueue.deadLetterSize())
                .processedAt(processedAt)
                .build();
    }

    private enum LifecycleAction {
        ACK,
        NACK
    }
}


