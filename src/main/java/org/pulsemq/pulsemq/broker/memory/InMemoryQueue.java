package org.pulsemq.pulsemq.broker.memory;

import lombok.Getter;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.QueueEntity;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class InMemoryQueue {

    private final UUID queueId;
    private final String queueName;
    private final QueueType queueType;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final BlockingQueue<QueuedMessage> buffer;

    public InMemoryQueue(QueueEntity queueEntity) {
        this(
                queueEntity.getId(),
                queueEntity.getName(),
                queueEntity.getType(),
                queueEntity.getCreatedAt(),
                queueEntity.getUpdatedAt(),
                new LinkedBlockingQueue<>()
        );
    }

    public InMemoryQueue(
            UUID queueId,
            String queueName,
            QueueType queueType,
            Instant createdAt,
            Instant updatedAt,
            BlockingQueue<QueuedMessage> buffer
    ) {
        this.queueId = Objects.requireNonNull(queueId, "queueId must not be null");
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null");
        this.queueType = Objects.requireNonNull(queueType, "queueType must not be null");
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
    }

    public boolean enqueue(QueuedMessage queuedMessage) {
        Objects.requireNonNull(queuedMessage, "queuedMessage must not be null");
        return buffer.offer(queuedMessage);
    }

    public QueuedMessage poll() {
        return buffer.poll();
    }

    public int clear() {
        int size = buffer.size();
        buffer.clear();
        return size;
    }

    public int size() {
        return buffer.size();
    }
}

