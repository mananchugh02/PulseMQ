package org.pulsemq.pulsemq.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.ResponseDTO.QueueMessageResponseDTO;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageConsumeService {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final QueueRepository queueRepository;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;
    private final MessageConsumeStateService messageConsumeStateService;
    private final ExecutorService consumerWaitExecutor;

    public Optional<QueueMessageResponseDTO> consumeMessage(UUID queueId, Long timeoutSeconds) {
        validate(queueId, timeoutSeconds);

        QueueEntity queueEntity = queueRepository.getQueueById(queueId)
                .orElseThrow(() -> new EntityNotFoundException("Queue not found for id: " + queueId));

        InMemoryQueue runtimeQueue = inMemoryQueueRegistry.getQueue(queueId)
                .orElseGet(() -> inMemoryQueueRegistry.registerQueue(queueEntity));

        Duration timeout = timeoutSeconds == null ? DEFAULT_TIMEOUT : Duration.ofSeconds(timeoutSeconds);
        log.info("Consumer wait started for queue {} with timeout {}s", queueId, timeout.toSeconds());

        Future<QueuedMessage> waitingMessage = consumerWaitExecutor.submit(runtimeQueue::takeReadyMessage);
        QueuedMessage readyMessage;
        try {
            readyMessage = waitingMessage.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            waitingMessage.cancel(true);
            log.info("Consumer timeout for queue {} after {}s", queueId, timeout.toSeconds());
            return Optional.empty();
        } catch (InterruptedException e) {
            waitingMessage.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Consumer wait interrupted for queue " + queueId, e);
        } catch (ExecutionException e) {
            waitingMessage.cancel(true);
            throw new IllegalStateException("Failed while waiting for message from queue " + queueId, e.getCause());
        }

        log.info("Consumer resumed for queue {} with message {}", queueId, readyMessage.getMessageId());
        return Optional.of(messageConsumeStateService.finalizeConsume(queueId, readyMessage));
    }

    private void validate(UUID queueId, Long timeoutSeconds) {
        if (queueId == null) {
            throw new IllegalArgumentException("Queue id must not be null");
        }
        if (timeoutSeconds != null && timeoutSeconds < 0) {
            throw new IllegalArgumentException("Timeout must not be negative");
        }
    }
}




