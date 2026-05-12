package org.pulsemq.pulsemq.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pulsemq.pulsemq.DTO.ResponseDTO.QueueMessageResponseDTO;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.pulsemq.pulsemq.service.wal.WalEventRecorder;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageConsumeServiceTest {

	@Mock
	private QueueRepository queueRepository;

	@Mock
	private MessageConsumeStateService messageConsumeStateService;

	@Mock
	private WalEventRecorder walEventRecorder;

	private InMemoryQueueRegistry inMemoryQueueRegistry;
	private ExecutorService executorService;
	private MessageConsumeService messageConsumeService;

	@BeforeEach
	void setUp() {
		inMemoryQueueRegistry = new InMemoryQueueRegistry(walEventRecorder);
		executorService = Executors.newSingleThreadExecutor();
		messageConsumeService = new MessageConsumeService(
				queueRepository,
				inMemoryQueueRegistry,
				messageConsumeStateService,
				executorService
		);
	}

	@AfterEach
	void tearDown() {
		executorService.shutdownNow();
	}

	@Test
	void consumeMessageReturnsMessageWhenAvailable() {
		QueueEntity queue = queue("orders.main");
		UUID messageId = UUID.randomUUID();

		InMemoryQueue runtime = inMemoryQueueRegistry.registerQueue(queue);
		runtime.enqueue(QueuedMessage.builder()
				.messageId(messageId)
				.queueId(queue.getId())
				.payload("payload")
				.headers(java.util.Map.of())
				.enqueuedAt(Instant.now())
				.build());

		QueueMessageResponseDTO responseDTO = QueueMessageResponseDTO.builder()
				.id(messageId)
				.queueId(queue.getId())
				.payload("payload")
				.status(MessageStatus.IN_FLIGHT)
				.retryCount(0)
				.visibleAt(Instant.now())
				.createdAt(Instant.now())
				.updatedAt(Instant.now())
				.build();

		when(queueRepository.getQueueById(queue.getId())).thenReturn(Optional.of(queue));
		when(messageConsumeStateService.finalizeConsume(eq(queue.getId()), any(QueuedMessage.class))).thenReturn(responseDTO);

		Optional<QueueMessageResponseDTO> result = messageConsumeService.consumeMessage(queue.getId(), 1L);

		assertTrue(result.isPresent());
		assertEquals(messageId, result.get().getId());
	}

	@Test
	void consumeMessageReturnsEmptyOnTimeout() {
		QueueEntity queue = queue("orders.main");
		inMemoryQueueRegistry.registerQueue(queue);

		when(queueRepository.getQueueById(queue.getId())).thenReturn(Optional.of(queue));

		Optional<QueueMessageResponseDTO> result = messageConsumeService.consumeMessage(queue.getId(), 0L);

		assertFalse(result.isPresent());
	}

	private QueueEntity queue(String name) {
		return QueueEntity.builder()
				.id(UUID.randomUUID())
				.name(name)
				.type(QueueType.MAIN)
				.createdAt(Instant.now())
				.updatedAt(Instant.now())
				.build();
	}
}

