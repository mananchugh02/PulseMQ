package org.pulsemq.pulsemq.broker.memory;

import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class InMemoryQueueRegistry {

    private final ConcurrentMap<UUID, InMemoryQueue> queues = new ConcurrentHashMap<>();

    public synchronized void replaceAll(Collection<QueueEntity> queueEntities) {
        ConcurrentMap<UUID, InMemoryQueue> refreshedQueues = new ConcurrentHashMap<>();
        if (queueEntities != null) {
            queueEntities.stream()
                    .filter(queueEntity -> queueEntity != null && queueEntity.getId() != null)
                    .forEach(queueEntity -> {
                        InMemoryQueue existingQueue = queues.get(queueEntity.getId());
                        BlockingQueue<QueuedMessage> buffer = existingQueue != null
                                ? existingQueue.getBuffer()
                                : new LinkedBlockingQueue<>();

                        refreshedQueues.put(
                                queueEntity.getId(),
                                new InMemoryQueue(
                                        queueEntity.getId(),
                                        queueEntity.getName(),
                                        queueEntity.getType(),
                                        queueEntity.getCreatedAt(),
                                        queueEntity.getUpdatedAt(),
                                        buffer
                                )
                        );
                    });
        }

        queues.clear();
        queues.putAll(refreshedQueues);
    }

    public InMemoryQueue registerQueue(QueueEntity queueEntity) {
        if (queueEntity == null || queueEntity.getId() == null) {
            throw new IllegalArgumentException("Queue entity and id must not be null");
        }

        InMemoryQueue existingQueue = queues.get(queueEntity.getId());
        BlockingQueue<QueuedMessage> buffer = existingQueue != null
                ? existingQueue.getBuffer()
                : new LinkedBlockingQueue<>();

        InMemoryQueue inMemoryQueue = new InMemoryQueue(
                queueEntity.getId(),
                queueEntity.getName(),
                queueEntity.getType(),
                queueEntity.getCreatedAt(),
                queueEntity.getUpdatedAt(),
                buffer
        );
        queues.put(queueEntity.getId(), inMemoryQueue);
        return inMemoryQueue;
    }

    public Optional<InMemoryQueue> getQueue(UUID queueId) {
        if (queueId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(queues.get(queueId));
    }

    public List<InMemoryQueue> getAllQueues() {
        return List.copyOf(queues.values());
    }

    public boolean containsQueue(UUID queueId) {
        return queueId != null && queues.containsKey(queueId);
    }

    public void removeQueue(UUID queueId) {
        if (queueId != null) {
            queues.remove(queueId);
        }
    }

    public int size() {
        return queues.size();
    }
}


