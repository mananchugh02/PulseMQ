package org.pulsemq.pulsemq.psql.repository;

import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface QueueRepository extends JpaRepository<QueueEntity, UUID> {

    default QueueEntity createQueue(QueueEntity queueEntity) {
        return save(queueEntity);
    }

    default QueueEntity updateQueue(QueueEntity queueEntity) {
        return save(queueEntity);
    }

    default Optional<QueueEntity> getQueueById(UUID queueId) {
        return findById(queueId);
    }

    default void deleteQueue(QueueEntity queueEntity) {
        delete(queueEntity);
    }

    default void deleteQueueById(UUID queueId) {
        deleteById(queueId);
    }

    Optional<QueueEntity> findByName(String name);

    List<QueueEntity> findAllByNameContainingIgnoreCase(String name);

    List<QueueEntity> findAllByType(QueueType type);
}

