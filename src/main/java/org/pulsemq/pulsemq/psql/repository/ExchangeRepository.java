package org.pulsemq.pulsemq.psql.repository;

import org.pulsemq.pulsemq.common.enums.ExchangeType;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ExchangeRepository extends JpaRepository<ExchangeEntity, UUID> {

    default ExchangeEntity createExchange(ExchangeEntity exchangeEntity) {
        return save(exchangeEntity);
    }

    default ExchangeEntity updateExchange(ExchangeEntity exchangeEntity) {
        return save(exchangeEntity);
    }

    default Optional<ExchangeEntity> getExchangeById(UUID exchangeId) {
        return findById(exchangeId);
    }

    default void deleteExchange(ExchangeEntity exchangeEntity) {
        delete(exchangeEntity);
    }

    default void deleteExchangeById(UUID exchangeId) {
        deleteById(exchangeId);
    }

    Optional<ExchangeEntity> findByName(String name);

    List<ExchangeEntity> findAllByNameContainingIgnoreCase(String name);

    List<ExchangeEntity> findAllByType(ExchangeType type);
}

