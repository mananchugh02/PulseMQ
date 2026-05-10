package org.pulsemq.pulsemq.psql.repository;

import org.pulsemq.pulsemq.psql.model.BindingEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface BindingRepository extends JpaRepository<BindingEntity, UUID> {

    default BindingEntity createBinding(BindingEntity bindingEntity) {
        return save(bindingEntity);
    }

    default BindingEntity updateBinding(BindingEntity bindingEntity) {
        return save(bindingEntity);
    }

    default Optional<BindingEntity> getBindingById(UUID bindingId) {
        return findById(bindingId);
    }

    default void deleteBinding(BindingEntity bindingEntity) {
        delete(bindingEntity);
    }

    default void deleteBindingById(UUID bindingId) {
        deleteById(bindingId);
    }

    Optional<BindingEntity> findByExchange_IdAndQueue_IdAndRoutingKey(UUID exchangeId, UUID queueId, String routingKey);

    List<BindingEntity> findAllByExchange_Id(UUID exchangeId);

    List<BindingEntity> findAllByQueue_Id(UUID queueId);

    List<BindingEntity> findAllByRoutingKeyContainingIgnoreCase(String routingKey);

    List<BindingEntity> findAllByExchange_NameContainingIgnoreCaseOrQueue_NameContainingIgnoreCaseOrRoutingKeyContainingIgnoreCase(
            String exchangeName,
            String queueName,
            String routingKey
    );
}

