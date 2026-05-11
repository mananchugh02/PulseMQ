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
import org.pulsemq.pulsemq.service.wal.WalEvent;
import org.pulsemq.pulsemq.service.wal.WalEventRecorder;

@Getter
public class InMemoryQueue {

    private final UUID queueId;
    private final String queueName;
    private final QueueType queueType;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final BlockingQueue<QueuedMessage> buffer;
    private final ConcurrentMap<UUID, QueuedMessage> inFlightMessages;
    private final WalEventRecorder walEventRecorder;

    public InMemoryQueue(QueueEntity queueEntity) {
        this(
                queueEntity.getId(),
                queueEntity.getName(),
                queueEntity.getType(),
                queueEntity.getCreatedAt(),
                queueEntity.getUpdatedAt(),
                new LinkedBlockingQueue<>(),
                new ConcurrentHashMap<>(),
                null
        );
    }

    public InMemoryQueue(QueueEntity queueEntity, WalEventRecorder walEventRecorder) {
        this(
                queueEntity.getId(),
                queueEntity.getName(),
                queueEntity.getType(),
                queueEntity.getCreatedAt(),
                queueEntity.getUpdatedAt(),
                new LinkedBlockingQueue<>(),
                new ConcurrentHashMap<>(),
                walEventRecorder
        );
    }

    public InMemoryQueue(
            UUID queueId,
            String queueName,
            QueueType queueType,
            Instant createdAt,
            Instant updatedAt,
            BlockingQueue<QueuedMessage> buffer,
            ConcurrentMap<UUID, QueuedMessage> inFlightMessages
    ) {
        this(queueId, queueName, queueType, createdAt, updatedAt, buffer, inFlightMessages, null);
    }

    public InMemoryQueue(
            UUID queueId,
            String queueName,
            QueueType queueType,
            Instant createdAt,
            Instant updatedAt,
            BlockingQueue<QueuedMessage> buffer,
            ConcurrentMap<UUID, QueuedMessage> inFlightMessages,
            WalEventRecorder walEventRecorder
    ) {
        this.queueId = Objects.requireNonNull(queueId, "queueId must not be null");
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null");
        this.queueType = Objects.requireNonNull(queueType, "queueType must not be null");
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.inFlightMessages = Objects.requireNonNull(inFlightMessages, "inFlightMessages must not be null");
        this.walEventRecorder = walEventRecorder;
    }

    public boolean enqueue(QueuedMessage queuedMessage) {
        Objects.requireNonNull(queuedMessage, "queuedMessage must not be null");
        synchronized (this) {
            return buffer.offer(queuedMessage);
        }
    }

    public QueuedMessage takeReadyMessage() throws InterruptedException {
        return buffer.take();
    }

    // dead-letter queues are regular queues now; enqueue into the DLQ's runtime queue via registry

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
            readyMessage.ifPresent(this::claimTakenMessage);
            return readyMessage;
        }
    }

    public QueuedMessage claimTakenMessage(QueuedMessage readyMessage) {
        Objects.requireNonNull(readyMessage, "readyMessage must not be null");
        synchronized (this) {
            QueuedMessage inflightMessage = toInflightMessage(readyMessage);
            inFlightMessages.put(readyMessage.getMessageId(), inflightMessage);
            if (walEventRecorder != null) {
                walEventRecorder.record(WalEvent.claim(inflightMessage, toQueueEntity(), inflightMessage.getInflightAt()));
            }
            return inflightMessage;
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

    public void restoreReadyMessage(QueuedMessage queuedMessage) {
        Objects.requireNonNull(queuedMessage, "queuedMessage must not be null");
        synchronized (this) {
            inFlightMessages.remove(queuedMessage.getMessageId());
            removeFromQueue(buffer, queuedMessage.getMessageId());
            if (!buffer.offer(queuedMessage)) {
                throw new IllegalStateException("Unable to restore ready message into runtime queue " + queueId);
            }
        }
    }

    public void restoreInflightMessage(QueuedMessage queuedMessage) {
        Objects.requireNonNull(queuedMessage, "queuedMessage must not be null");
        synchronized (this) {
            removeFromQueue(buffer, queuedMessage.getMessageId());
            inFlightMessages.put(queuedMessage.getMessageId(), queuedMessage);
        }
    }

    public Optional<QueuedMessage> removeDeadLetterMessage(UUID messageId) {
        throw new UnsupportedOperationException("Dead letter messages are stored in their own runtime queue");
    }

    public int clear() {
        synchronized (this) {
            int size = buffer.size();
            buffer.clear();
            return size;
        }
    }

    public int clearDeadLetter() {
        throw new UnsupportedOperationException("Dead letter queue is a regular queue; use its queue id to clear");
    }

    public int size() {
        return buffer.size();
    }

    public int deadLetterSize() {
        throw new UnsupportedOperationException("Dead letter queue is a regular queue; use its queue id to get size");
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

    private QueueEntity toQueueEntity() {
        return QueueEntity.builder()
                .id(queueId)
                .name(queueName)
                .type(queueType)
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .build();
    }

    private QueuedMessage toInflightMessage(QueuedMessage message) {
        return QueuedMessage.builder()
                .messageId(message.getMessageId())
                .queueId(message.getQueueId())
                .routingKey(message.getRoutingKey())
                .payload(message.getPayload())
                .headers(message.getHeaders())
                .enqueuedAt(message.getEnqueuedAt())
                .inflightAt(Instant.now())
                .build();
    }
}

