package org.pulsemq.pulsemq.psql.mapper;

import org.pulsemq.pulsemq.psql.dto.ExchangeEntityDTO;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.springframework.stereotype.Component;

@Component
public class ExchangeEntityMapper {

    public ExchangeEntityDTO toDTO(ExchangeEntity exchangeEntity) {
        if (exchangeEntity == null) {
            return null;
        }
        return ExchangeEntityDTO.builder()
                .id(exchangeEntity.getId())
                .name(exchangeEntity.getName())
                .type(exchangeEntity.getType())
                .createdAt(exchangeEntity.getCreatedAt())
                .updatedAt(exchangeEntity.getUpdatedAt())
                .build();
    }

    public ExchangeEntity toEntity(ExchangeEntityDTO exchangeEntityDTO) {
        if (exchangeEntityDTO == null) {
            return null;
        }
        return ExchangeEntity.builder()
                .id(exchangeEntityDTO.getId())
                .name(exchangeEntityDTO.getName())
                .type(exchangeEntityDTO.getType())
                .createdAt(exchangeEntityDTO.getCreatedAt())
                .updatedAt(exchangeEntityDTO.getUpdatedAt())
                .build();
    }
}

