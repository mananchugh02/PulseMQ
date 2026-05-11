package org.pulsemq.pulsemq.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.pulsemq.pulsemq.DTO.ResponseDTO.QueueMessageResponseDTO;
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
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class MessageConsumeStateService {

    private final QueueRepository queueRepository;
    private final MessageRepository messageRepository;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;

    @Transactional
    public QueueMessageResponseDTO finalizeConsume(UUID queueId, QueuedMessage readyMessage) {
        QueueEntity queueEntity = queueRepository.getQueueById(queueId)
                .orElseThrow(() -> new EntityNotFoundException("Queue not found for id: " + queueId));

        MessageEntity messageEntity = messageRepository.findByIdAndQueue_Id(readyMessage.getMessageId(), queueId)
                .orElseThrow(() -> new EntityNotFoundException("Message not found for id: " + readyMessage.getMessageId() + " in queue: " + queueId));

        InMemoryQueue runtimeQueue = inMemoryQueueRegistry.getQueue(queueId)
                .orElseGet(() -> inMemoryQueueRegistry.registerQueue(queueEntity));

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                if (status != TransactionSynchronization.STATUS_COMMITTED) {
                    runtimeQueue.restoreReadyMessage(readyMessage);
                }
            }
        });

        runtimeQueue.claimTakenMessage(readyMessage);
        Instant now = Instant.now();
        messageEntity.setStatus(MessageStatus.IN_FLIGHT);
        messageEntity.setVisibleAt(now);
        MessageEntity saved = messageRepository.save(messageEntity);

        return QueueMessageResponseDTO.builder()
                .id(saved.getId())
                .queueId(queueEntity.getId())
                .payload(saved.getPayload())
                .status(saved.getStatus())
                .retryCount(saved.getRetryCount())
                .visibleAt(saved.getVisibleAt())
                .createdAt(saved.getCreatedAt())
                .updatedAt(saved.getUpdatedAt())
                .build();
    }
}



