package org.pulsemq.pulsemq.psql.mapper;

import org.pulsemq.pulsemq.psql.model.BindingEntity;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.dto.BindingEntityDTO;
import org.springframework.stereotype.Component;

@Component
public class BindingEntityMapper {

    public BindingEntityDTO toDTO(BindingEntity bindingEntity) {
        if (bindingEntity == null) {
            return null;
        }
        return BindingEntityDTO.builder()
                .id(bindingEntity.getId())
                .exchangeId(bindingEntity.getExchange() != null ? bindingEntity.getExchange().getId() : null)
                .queueId(bindingEntity.getQueue() != null ? bindingEntity.getQueue().getId() : null)
                .routingKey(bindingEntity.getRoutingKey())
                .createdAt(bindingEntity.getCreatedAt())
                .build();
    }

    public BindingEntity toEntity(BindingEntityDTO bindingEntityDTO) {
        if (bindingEntityDTO == null) {
            return null;
        }
        // When converting from DTO to Entity, we typically only have the IDs for related entities.
        // The actual ExchangeEntity and QueueEntity objects would need to be fetched from a repository
        // or provided separately if the full entity graph is required.
        // For now, we'll create placeholder entities with just the IDs.
        ExchangeEntity exchangeEntity = null;
        if (bindingEntityDTO.getExchangeId() != null) {
            exchangeEntity = ExchangeEntity.builder().id(bindingEntityDTO.getExchangeId()).build();
        }

        QueueEntity queueEntity = null;
        if (bindingEntityDTO.getQueueId() != null) {
            queueEntity = QueueEntity.builder().id(bindingEntityDTO.getQueueId()).build();
        }

        return BindingEntity.builder()
                .id(bindingEntityDTO.getId())
                .exchange(exchangeEntity)
                .queue(queueEntity)
                .routingKey(bindingEntityDTO.getRoutingKey())
                .createdAt(bindingEntityDTO.getCreatedAt())
                .build();
    }
}