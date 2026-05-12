package org.pulsemq.pulsemq.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PurgeQueueResponseDTO;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.mapper.QueueEntityMapper;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.pulsemq.pulsemq.service.QueueMetricsService;
import org.pulsemq.pulsemq.service.wal.WalEventRecorder;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueServiceImplTest {

	@Mock
	private QueueRepository queueRepository;

	@Mock
	private QueueEntityMapper queueEntityMapper;

	@Mock
	private MessageRepository messageRepository;

	@Mock
	private QueueMetricsService queueMetricsService;

	@Mock
	private WalEventRecorder walEventRecorder;

	private InMemoryQueueRegistry inMemoryQueueRegistry;
	private QueueServiceImpl queueService;

	@BeforeEach
	void setUp() {
		inMemoryQueueRegistry = new InMemoryQueueRegistry(walEventRecorder);
		queueService = new QueueServiceImpl();

		ReflectionTestUtils.setField(queueService, "queueRepository", queueRepository);
		ReflectionTestUtils.setField(queueService, "queueEntityMapper", queueEntityMapper);
		ReflectionTestUtils.setField(queueService, "inMemoryQueueRegistry", inMemoryQueueRegistry);
		ReflectionTestUtils.setField(queueService, "messageRepository", messageRepository);
		ReflectionTestUtils.setField(queueService, "queueMetricsService", queueMetricsService);
	}

	@Test
	void purgeQueueClearsReadyInflightAndRetryPendingStates() {
		QueueEntity queue = QueueEntity.builder()
				.id(UUID.randomUUID())
				.name("orders.main")
				.type(QueueType.MAIN)
				.createdAt(Instant.now())
				.updatedAt(Instant.now())
				.build();

		InMemoryQueue runtime = inMemoryQueueRegistry.registerQueue(queue);

		runtime.enqueue(QueuedMessage.builder()
				.messageId(UUID.randomUUID())
				.queueId(queue.getId())
				.payload("ready")
				.headers(java.util.Map.of())
				.enqueuedAt(Instant.now())
				.build());

		runtime.restoreInflightMessage(QueuedMessage.builder()
				.messageId(UUID.randomUUID())
				.queueId(queue.getId())
				.payload("inflight")
				.headers(java.util.Map.of())
				.enqueuedAt(Instant.now())
				.inflightAt(Instant.now())
				.build());

		when(queueRepository.getQueueById(queue.getId())).thenReturn(Optional.of(queue));
		when(messageRepository.updateStatusByQueue_IdAndStatus(queue.getId(), MessageStatus.READY, MessageStatus.PURGED)).thenReturn(2);
		when(messageRepository.updateStatusByQueue_IdAndStatus(queue.getId(), MessageStatus.IN_FLIGHT, MessageStatus.PURGED)).thenReturn(1);
		when(messageRepository.updateStatusByQueue_IdAndStatus(queue.getId(), MessageStatus.RETRY_PENDING, MessageStatus.PURGED)).thenReturn(3);

		PurgeQueueResponseDTO response = queueService.purgeQueue(queue.getId());

		assertEquals(6, response.getDatabaseMessagesPurged());
		assertEquals(2, response.getInMemoryMessagesCleared());
		assertEquals(0, runtime.size());
		assertEquals(0, runtime.inFlightSize());
		assertEquals("PURGED", response.getStatus());
	}
}

