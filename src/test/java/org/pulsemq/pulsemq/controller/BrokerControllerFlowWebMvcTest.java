package org.pulsemq.pulsemq.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.PublishMessageRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.MessageLifecycleResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageDeliveryDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.QueueMessageResponseDTO;
import org.pulsemq.pulsemq.common.enums.ExchangeType;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.controller.impl.BindingControllerImpl;
import org.pulsemq.pulsemq.controller.impl.ExchangeControllerImpl;
import org.pulsemq.pulsemq.controller.impl.MessageControllerImpl;
import org.pulsemq.pulsemq.controller.impl.QueueControllerImpl;
import org.pulsemq.pulsemq.psql.service.MessageLifecycleService;
import org.pulsemq.pulsemq.broker.publish.MessagePublishService;
import org.pulsemq.pulsemq.service.BindingService;
import org.pulsemq.pulsemq.service.ExchangeService;
import org.pulsemq.pulsemq.service.MessageConsumeService;
import org.pulsemq.pulsemq.service.QueueService;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class BrokerControllerFlowWebMvcTest {

    private MockMvc mockMvc;

    private ObjectMapper objectMapper;

    @Mock
    private QueueService queueService;

    @Mock
    private ExchangeService exchangeService;

    @Mock
    private BindingService bindingService;

    @Mock
    private MessagePublishService messagePublishService;

    @Mock
    private MessageLifecycleService messageLifecycleService;

    @Mock
    private MessageConsumeService messageConsumeService;

    @BeforeEach
    void setUp() {
        QueueControllerImpl queueController = new QueueControllerImpl();
        ExchangeControllerImpl exchangeController = new ExchangeControllerImpl();
        BindingControllerImpl bindingController = new BindingControllerImpl();
        MessageControllerImpl messageController = new MessageControllerImpl(messagePublishService, messageLifecycleService);

        ReflectionTestUtils.setField(queueController, "queueService", queueService);
        ReflectionTestUtils.setField(queueController, "messageConsumeService", messageConsumeService);
        ReflectionTestUtils.setField(exchangeController, "exchangeService", exchangeService);
        ReflectionTestUtils.setField(bindingController, "bindingService", bindingService);

        mockMvc = MockMvcBuilders
                .standaloneSetup(queueController, exchangeController, bindingController, messageController)
                .build();

        objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Test
    void createBindPublishConsumeAckFlowReturnsExpectedResponses() throws Exception {
        UUID queueId = UUID.randomUUID();
        UUID exchangeId = UUID.randomUUID();
        UUID bindingId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        Instant now = Instant.now();

        CreateQueueResponseDTO queueResponse = CreateQueueResponseDTO.builder()
                .id(queueId)
                .name("orders.main")
                .type(QueueType.MAIN)
                .createdAt(now)
                .updatedAt(now)
                .build();

        CreateExchangeResponseDTO exchangeResponse = CreateExchangeResponseDTO.builder()
                .id(exchangeId)
                .name("orders.exchange")
                .type(ExchangeType.DIRECT)
                .createdAt(now)
                .updatedAt(now)
                .build();

        CreateBindingResponseDTO bindingResponse = CreateBindingResponseDTO.builder()
                .id(bindingId)
                .exchangeId(exchangeId)
                .queueId(queueId)
                .routingKey("orders.created")
                .createdAt(now)
                .build();

        PublishMessageResponseDTO publishResponse = PublishMessageResponseDTO.builder()
                .exchangeId(exchangeId)
                .exchangeName("orders.exchange")
                .exchangeType("DIRECT")
                .routingKey("orders.created")
                .matchedQueues(1)
                .savedMessages(1)
                .publishedAt(now)
                .deliveries(List.of(
                        PublishMessageDeliveryDTO.builder()
                                .queueId(queueId)
                                .queueName("orders.main")
                                .messageId(messageId)
                                .status(MessageStatus.READY)
                                .visibleAt(now)
                                .build()
                ))
                .build();

        QueueMessageResponseDTO consumeResponse = QueueMessageResponseDTO.builder()
                .id(messageId)
                .queueId(queueId)
                .payload("{\"orderId\":\"ORD-101\"}")
                .status(MessageStatus.IN_FLIGHT)
                .retryCount(0)
                .visibleAt(now)
                .createdAt(now)
                .updatedAt(now)
                .build();

        MessageLifecycleResponseDTO ackResponse = MessageLifecycleResponseDTO.builder()
                .queueId(queueId)
                .queueName("orders.main")
                .messageId(messageId)
                .action("ACK")
                .status(MessageStatus.ACKED)
                .retryCount(0)
                .deadLettered(false)
                .readyQueueSize(0)
                .deadLetterQueueSize(0)
                .processedAt(now)
                .build();

        when(queueService.createQueue(any(CreateQueueRequestDTO.class))).thenReturn(queueResponse);
        when(exchangeService.createExchange(any(CreateExchangeRequestDTO.class))).thenReturn(exchangeResponse);
        when(bindingService.createBinding(any(CreateBindingRequestDTO.class))).thenReturn(bindingResponse);
        when(messagePublishService.publishMessage(any(PublishMessageRequestDTO.class))).thenReturn(publishResponse);
        when(messageConsumeService.consumeMessage(queueId, 5L)).thenReturn(Optional.of(consumeResponse));
        when(messageLifecycleService.ackMessage(queueId, messageId)).thenReturn(ackResponse);

        mockMvc.perform(post("/api/queues/createQueue")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of("name", "orders.main", "type", "MAIN"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(queueId.toString()))
                .andExpect(jsonPath("$.name").value("orders.main"));

        mockMvc.perform(post("/api/exchanges/createExchange")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of("name", "orders.exchange", "type", "DIRECT"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(exchangeId.toString()))
                .andExpect(jsonPath("$.type").value("DIRECT"));

        mockMvc.perform(post("/api/bindings/createBinding")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of(
                                "exchangeId", exchangeId,
                                "queueId", queueId,
                                "routingKey", "orders.created"
                        ))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(bindingId.toString()))
                .andExpect(jsonPath("$.routingKey").value("orders.created"));

        mockMvc.perform(post("/api/messages/publish")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of(
                                "exchangeId", exchangeId,
                                "payload", "{\"orderId\":\"ORD-101\"}",
                                "routingKey", "orders.created",
                                "headers", Map.of("traceId", "tr-101")
                        ))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matchedQueues").value(1))
                .andExpect(jsonPath("$.deliveries[0].messageId").value(messageId.toString()));

        mockMvc.perform(get("/api/queues/{queueId}/consume", queueId)
                        .param("timeout", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(messageId.toString()))
                .andExpect(jsonPath("$.status").value("IN_FLIGHT"));

        mockMvc.perform(post("/api/messages/{queueId}/{messageId}/ack", queueId, messageId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.action").value("ACK"))
                .andExpect(jsonPath("$.status").value("ACKED"));
    }

    @Test
    void consumeEndpointReturnsNoContentOnTimeout() throws Exception {
        UUID queueId = UUID.randomUUID();
        when(messageConsumeService.consumeMessage(queueId, 1L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/queues/{queueId}/consume", queueId)
                        .param("timeout", "1"))
                .andExpect(status().isNoContent());
    }

    @Test
    void nackEndpointReturnsConflictWhenMessageIsNotInflight() throws Exception {
        UUID queueId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();

        when(messageLifecycleService.nackMessage(queueId, messageId))
                .thenThrow(new IllegalStateException("Message is not in IN_FLIGHT state"));

        mockMvc.perform(post("/api/messages/{queueId}/{messageId}/nack", queueId, messageId))
                .andExpect(status().isConflict());
    }
}

