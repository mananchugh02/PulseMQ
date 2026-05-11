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

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RetryService {

    private final InMemoryQueueRegistry inMemoryQueueRegistry;
    private final MessageRepository messageRepository;
    private final org.pulsemq.pulsemq.service.QueueMetricsService queueMetricsService;
    private final WalEventRecorder walEventRecorder;

    @Transactional
    public void processPendingRetries() {
        Instant now = Instant.now();
        List<MessageEntity> pending = messageRepository.findAllByStatusAndVisibleAtLessThanEqual(MessageStatus.RETRY_PENDING, now);
        if (pending == null || pending.isEmpty()) {
            return;
        }

        for (MessageEntity message : pending) {
            if (message == null) continue;
            QueueEntity queue = message.getQueue();
            if (queue == null) continue;

            InMemoryQueue inMemoryQueue = inMemoryQueueRegistry.getQueue(queue.getId())
                    .orElseGet(() -> inMemoryQueueRegistry.registerQueue(queue));

            // build runtime queued message and enqueue
            QueuedMessage queuedMessage = QueuedMessage.builder()
                    .messageId(message.getId())
                    .queueId(queue.getId())
                    .routingKey(null)
                    .payload(message.getPayload())
                    .headers(java.util.Map.of())
                    .enqueuedAt(Instant.now())
                    .build();

            boolean offered = inMemoryQueue.enqueue(queuedMessage);

            // update DB status back to READY and keep retryCount
            message.setStatus(MessageStatus.READY);
            message.setVisibleAt(Instant.now());
            messageRepository.save(message);

            walEventRecorder.record(WalEvent.retry(queuedMessage, queue, Instant.now()));

            log.info("Executed retry for message {} queue {} retryCount={} requeued={}",
                    message.getId(), queue.getName(), message.getRetryCount(), offered);
            // metrics: when messages are requeued from retry pending they are consumed again later; update published counter
            queueMetricsService.incrementPublished(queue.getId().toString());
        }
    }
}

