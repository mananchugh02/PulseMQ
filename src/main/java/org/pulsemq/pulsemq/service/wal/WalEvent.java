package org.pulsemq.pulsemq.service.wal;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.QueueEntity;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WalEvent {

    private WalEventType eventType;
    private UUID messageId;
    private UUID queueId;
    private String queueName;
    private QueueType queueType;
    private UUID targetQueueId;
    private String targetQueueName;
    private QueueType targetQueueType;
    private String routingKey;
    private String payload;
    private Map<String, String> headers;
    private Integer retryCount;
    private Instant timestamp;
    private Instant inflightAt;
    private Instant visibleAt;

    public static WalEvent publish(QueuedMessage message, QueueEntity queue, Integer retryCount, Instant timestamp) {
        return baseBuilder(WalEventType.PUBLISH, message, queue, timestamp)
                .retryCount(retryCount)
                .build();
    }

    public static WalEvent claim(QueuedMessage message, QueueEntity queue, Instant timestamp) {
        return baseBuilder(WalEventType.CLAIM, message, queue, timestamp)
                .inflightAt(timestamp)
                .build();
    }

    public static WalEvent ack(QueuedMessage message, QueueEntity queue, Instant timestamp) {
        return baseBuilder(WalEventType.ACK, message, queue, timestamp)
                .build();
    }

    public static WalEvent nack(QueuedMessage message, QueueEntity queue, int retryCount, Instant timestamp) {
        return baseBuilder(WalEventType.NACK, message, queue, timestamp)
                .retryCount(retryCount)
                .build();
    }

    public static WalEvent retry(QueuedMessage message, QueueEntity queue, Instant timestamp) {
        return baseBuilder(WalEventType.RETRY, message, queue, timestamp)
                .visibleAt(timestamp)
                .build();
    }

    public static WalEvent timeoutRequeue(QueuedMessage message, QueueEntity queue, Instant timestamp) {
        return baseBuilder(WalEventType.TIMEOUT_REQUEUE, message, queue, timestamp)
                .visibleAt(timestamp)
                .build();
    }

    public static WalEvent dlqMove(QueuedMessage message,
                                   QueueEntity sourceQueue,
                                   QueueEntity targetQueue,
                                   int retryCount,
                                   Instant timestamp) {
        return baseBuilder(WalEventType.DLQ_MOVE, message, sourceQueue, timestamp)
                .retryCount(retryCount)
                .targetQueueId(targetQueue != null ? targetQueue.getId() : null)
                .targetQueueName(targetQueue != null ? targetQueue.getName() : null)
                .targetQueueType(targetQueue != null ? targetQueue.getType() : null)
                .build();
    }

    private static WalEventBuilder baseBuilder(WalEventType eventType, QueuedMessage message, QueueEntity queue, Instant timestamp) {
        return WalEvent.builder()
                .eventType(eventType)
                .messageId(message != null ? message.getMessageId() : null)
                .queueId(queue != null ? queue.getId() : null)
                .queueName(queue != null ? queue.getName() : null)
                .queueType(queue != null ? queue.getType() : null)
                .routingKey(message != null ? message.getRoutingKey() : null)
                .payload(message != null ? message.getPayload() : null)
                .headers(message != null && message.getHeaders() != null ? Map.copyOf(message.getHeaders()) : Map.of())
                .timestamp(timestamp != null ? timestamp : Instant.now());
    }
}

