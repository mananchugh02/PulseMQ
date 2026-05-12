package org.pulsemq.pulsemq.broker.publish;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pulsemq.pulsemq.DTO.RequestDTO.PublishMessageRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageResponseDTO;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.common.enums.ExchangeType;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.BindingEntity;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.BindingRepository;
import org.pulsemq.pulsemq.psql.repository.ExchangeRepository;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.pulsemq.pulsemq.service.QueueMetricsService;
import org.pulsemq.pulsemq.service.wal.WalEventRecorder;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessagePublishServiceTest {

    @Mock
    private ExchangeRepository exchangeRepository;

    @Mock
    private BindingRepository bindingRepository;

    @Mock
    private QueueRepository queueRepository;

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private QueueMetricsService queueMetricsService;

    @Mock
    private WalEventRecorder walEventRecorder;

    private InMemoryQueueRegistry inMemoryQueueRegistry;
    private MessagePublishService messagePublishService;

    @BeforeEach
    void setUp() {
        inMemoryQueueRegistry = new InMemoryQueueRegistry(walEventRecorder);
        messagePublishService = new MessagePublishService(
                exchangeRepository,
                bindingRepository,
                queueRepository,
                messageRepository,
                inMemoryQueueRegistry,
                queueMetricsService,
                walEventRecorder
        );

        lenient().doNothing().when(queueMetricsService).registerQueueMetrics(any());
        lenient().doNothing().when(queueMetricsService).incrementPublished(any());
        lenient().doNothing().when(walEventRecorder).record(any());
    }

    @Test
    void publishMessageRoutesOnlyMatchedDirectBindings() {
        UUID exchangeId = UUID.randomUUID();
        ExchangeEntity exchange = ExchangeEntity.builder()
                .id(exchangeId)
                .name("orders.exchange")
                .type(ExchangeType.DIRECT)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        QueueEntity targetQueue = queue("orders.main");
        QueueEntity otherQueue = queue("payments.main");

        BindingEntity matched = binding(exchange, targetQueue, "orders.created");
        BindingEntity nonMatched = binding(exchange, otherQueue, "payments.created");

        PublishMessageRequestDTO request = PublishMessageRequestDTO.builder()
                .exchangeId(exchangeId)
                .routingKey("orders.created")
                .payload("{\"orderId\":\"ORD-1\"}")
                .headers(Map.of("traceId", "tr-1"))
                .build();

        MessageEntity saved = MessageEntity.builder()
                .id(UUID.randomUUID())
                .queue(targetQueue)
                .payload(request.getPayload())
                .status(MessageStatus.READY)
                .retryCount(0)
                .visibleAt(Instant.now())
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        when(exchangeRepository.getExchangeById(exchangeId)).thenReturn(Optional.of(exchange));
        when(bindingRepository.findAllByExchange_Id(exchangeId)).thenReturn(List.of(matched, nonMatched));
        when(messageRepository.saveAll(any())).thenReturn(List.of(saved));

        PublishMessageResponseDTO response = messagePublishService.publishMessage(request);

        assertEquals(1, response.getMatchedQueues());
        assertEquals(1, response.getSavedMessages());
        assertEquals("orders.exchange", response.getExchangeName());
        assertEquals(1, inMemoryQueueRegistry.getQueue(targetQueue.getId()).orElseThrow().size());
    }

    @Test
    void publishMessageSupportsTopicWildcardRouting() {
        UUID exchangeId = UUID.randomUUID();
        ExchangeEntity exchange = ExchangeEntity.builder()
                .id(exchangeId)
                .name("events.exchange")
                .type(ExchangeType.TOPIC)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        QueueEntity queue = queue("audit.main");
        BindingEntity matched = binding(exchange, queue, "orders.*");

        PublishMessageRequestDTO request = PublishMessageRequestDTO.builder()
                .exchangeId(exchangeId)
                .routingKey("orders.created")
                .payload("payload")
                .headers(Map.of())
                .build();

        MessageEntity saved = MessageEntity.builder()
                .id(UUID.randomUUID())
                .queue(queue)
                .payload("payload")
                .status(MessageStatus.READY)
                .retryCount(0)
                .visibleAt(Instant.now())
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        when(exchangeRepository.getExchangeById(exchangeId)).thenReturn(Optional.of(exchange));
        when(bindingRepository.findAllByExchange_Id(exchangeId)).thenReturn(List.of(matched));
        when(messageRepository.saveAll(any())).thenReturn(List.of(saved));

        PublishMessageResponseDTO response = messagePublishService.publishMessage(request);

        assertEquals(1, response.getMatchedQueues());
        assertEquals(1, inMemoryQueueRegistry.getQueue(queue.getId()).orElseThrow().size());
    }

    @Test
    void publishMessageFailsWhenNoBindingMatches() {
        UUID exchangeId = UUID.randomUUID();
        ExchangeEntity exchange = ExchangeEntity.builder()
                .id(exchangeId)
                .name("orders.exchange")
                .type(ExchangeType.DIRECT)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        QueueEntity queue = queue("orders.main");
        BindingEntity nonMatched = binding(exchange, queue, "orders.updated");

        PublishMessageRequestDTO request = PublishMessageRequestDTO.builder()
                .exchangeId(exchangeId)
                .routingKey("orders.created")
                .payload("payload")
                .headers(Map.of())
                .build();

        when(exchangeRepository.getExchangeById(exchangeId)).thenReturn(Optional.of(exchange));
        when(bindingRepository.findAllByExchange_Id(exchangeId)).thenReturn(List.of(nonMatched));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> messagePublishService.publishMessage(request));

        assertTrue(ex.getMessage().contains("No bindings matched"));
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

    private BindingEntity binding(ExchangeEntity exchange, QueueEntity queue, String routingKey) {
        return BindingEntity.builder()
                .id(UUID.randomUUID())
                .exchange(exchange)
                .queue(queue)
                .routingKey(routingKey)
                .createdAt(Instant.now())
                .build();
    }
}

