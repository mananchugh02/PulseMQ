package org.pulsemq.pulsemq.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.service.wal.WalEventRecorder;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetryAndVisibilityServiceTest {

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private QueueMetricsService queueMetricsService;

    @Mock
    private WalEventRecorder walEventRecorder;

    private InMemoryQueueRegistry inMemoryQueueRegistry;

    @BeforeEach
    void setUp() {
        inMemoryQueueRegistry = new InMemoryQueueRegistry(walEventRecorder);
        doNothing().when(queueMetricsService).incrementPublished(any());
        doNothing().when(walEventRecorder).record(any());
    }

    @Test
    void retryServiceRequeuesEligiblePendingMessages() {
        QueueEntity queue = queue("orders.main", QueueType.MAIN);
        InMemoryQueue runtime = inMemoryQueueRegistry.registerQueue(queue);

        MessageEntity pending = MessageEntity.builder()
                .id(UUID.randomUUID())
                .queue(queue)
                .payload("retry")
                .status(MessageStatus.RETRY_PENDING)
                .retryCount(2)
                .visibleAt(Instant.now().minusSeconds(1))
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        when(messageRepository.findAllByStatusAndVisibleAtLessThanEqual(eq(MessageStatus.RETRY_PENDING), any(Instant.class)))
                .thenReturn(List.of(pending));
        when(messageRepository.save(any(MessageEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        RetryService retryService = new RetryService(inMemoryQueueRegistry, messageRepository, queueMetricsService, walEventRecorder);
        retryService.processPendingRetries();

        assertEquals(MessageStatus.READY, pending.getStatus());
        assertEquals(1, runtime.size());
    }

    @Test
    void visibilityTimeoutServiceRequeuesTimedOutInflightMessages() {
        QueueEntity queue = queue("orders.main", QueueType.MAIN);
        InMemoryQueue runtime = inMemoryQueueRegistry.registerQueue(queue);

        UUID messageId = UUID.randomUUID();
        runtime.restoreInflightMessage(QueuedMessage.builder()
                .messageId(messageId)
                .queueId(queue.getId())
                .payload("payload")
                .headers(java.util.Map.of())
                .enqueuedAt(Instant.now().minusSeconds(40))
                .inflightAt(Instant.now().minusSeconds(40))
                .build());

        MessageEntity inflight = MessageEntity.builder()
                .id(messageId)
                .queue(queue)
                .payload("payload")
                .status(MessageStatus.IN_FLIGHT)
                .retryCount(0)
                .visibleAt(Instant.now().minusSeconds(40))
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        when(messageRepository.getMessageById(messageId)).thenReturn(Optional.of(inflight));
        when(messageRepository.save(any(MessageEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        VisibilityTimeoutService visibilityTimeoutService = new VisibilityTimeoutService(
                inMemoryQueueRegistry,
                messageRepository,
                queueMetricsService,
                walEventRecorder
        );
        visibilityTimeoutService.recoverTimedOutMessages();

        assertEquals(MessageStatus.READY, inflight.getStatus());
        assertEquals(0, runtime.inFlightSize());
        assertEquals(1, runtime.size());
    }

    private QueueEntity queue(String name, QueueType type) {
        return QueueEntity.builder()
                .id(UUID.randomUUID())
                .name(name)
                .type(type)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }
}

