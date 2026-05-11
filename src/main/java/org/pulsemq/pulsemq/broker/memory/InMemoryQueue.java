package org.pulsemq.pulsemq.broker.memory;

import lombok.Getter;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.QueueEntity;

import java.time.Instant;
import java.util.Optional;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class InMemoryQueue {

    private final UUID queueId;
    private final String queueName;
    private final QueueType queueType;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final BlockingQueue<QueuedMessage> buffer;
    private final BlockingQueue<QueuedMessage> deadLetterBuffer;
    private final ConcurrentMap<UUID, QueuedMessage> inFlightMessages;

    public InMemoryQueue(QueueEntity queueEntity) {
        this(
                queueEntity.getId(),
                queueEntity.getName(),
                queueEntity.getType(),
                queueEntity.getCreatedAt(),
                queueEntity.getUpdatedAt(),
                new LinkedBlockingQueue<>(),
                new LinkedBlockingQueue<>(),
                new ConcurrentHashMap<>()
        );
    }

    public InMemoryQueue(
            UUID queueId,
            String queueName,
            QueueType queueType,
            Instant createdAt,
            Instant updatedAt,
            BlockingQueue<QueuedMessage> buffer,
            BlockingQueue<QueuedMessage> deadLetterBuffer,
            ConcurrentMap<UUID, QueuedMessage> inFlightMessages
    ) {
        this.queueId = Objects.requireNonNull(queueId, "queueId must not be null");
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null");
        this.queueType = Objects.requireNonNull(queueType, "queueType must not be null");
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.deadLetterBuffer = Objects.requireNonNull(deadLetterBuffer, "deadLetterBuffer must not be null");
        this.inFlightMessages = Objects.requireNonNull(inFlightMessages, "inFlightMessages must not be null");
    }

    public boolean enqueue(QueuedMessage queuedMessage) {
        Objects.requireNonNull(queuedMessage, "queuedMessage must not be null");
        synchronized (this) {
            return buffer.offer(queuedMessage);
        }
    }

    public boolean enqueueDeadLetter(QueuedMessage queuedMessage) {
        Objects.requireNonNull(queuedMessage, "queuedMessage must not be null");
        synchronized (this) {
            return deadLetterBuffer.offer(queuedMessage);
        }
    }

    public QueuedMessage poll() {
        synchronized (this) {
            return buffer.poll();
        }
    }

    public Optional<QueuedMessage> claimMessage(UUID messageId) {
        if (messageId == null) {
            return Optional.empty();
        }

        QueuedMessage inflight = inFlightMessages.get(messageId);
        if (inflight != null) {
            return Optional.of(inflight);
        }

        synchronized (this) {
            inflight = inFlightMessages.get(messageId);
            if (inflight != null) {
                return Optional.of(inflight);
            }

            Optional<QueuedMessage> readyMessage = removeFromQueue(buffer, messageId);
            readyMessage.ifPresent(message -> inFlightMessages.put(messageId, message));
            return readyMessage;
        }
    }

    public Optional<QueuedMessage> completeProcessing(UUID messageId) {
        if (messageId == null) {
            return Optional.empty();
        }

        synchronized (this) {
            return Optional.ofNullable(inFlightMessages.remove(messageId));
        }
    }

    public Optional<QueuedMessage> removeReadyMessage(UUID messageId) {
        synchronized (this) {
            return removeFromQueue(buffer, messageId);
        }
    }

    public Optional<QueuedMessage> removeDeadLetterMessage(UUID messageId) {
        synchronized (this) {
            return removeFromQueue(deadLetterBuffer, messageId);
        }
    }

    public int clear() {
        synchronized (this) {
            int size = buffer.size();
            buffer.clear();
            return size;
        }
    }

    public int clearDeadLetter() {
        synchronized (this) {
            int size = deadLetterBuffer.size();
            deadLetterBuffer.clear();
            return size;
        }
    }

    public int size() {
        return buffer.size();
    }

    public int deadLetterSize() {
        return deadLetterBuffer.size();
    }

    public int inFlightSize() {
        return inFlightMessages.size();
    }

    private Optional<QueuedMessage> removeFromQueue(BlockingQueue<QueuedMessage> queue, UUID messageId) {
        for (QueuedMessage queuedMessage : queue) {
            if (messageId.equals(queuedMessage.getMessageId()) && queue.remove(queuedMessage)) {
                return Optional.of(queuedMessage);
            }
        }
        return Optional.empty();
    }
}

