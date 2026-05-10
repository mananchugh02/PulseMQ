package org.pulsemq.pulsemq.psql.mapper;

import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.dto.QueueEntityDTO;
import org.springframework.stereotype.Component;

@Component
public class QueueEntityMapper {

    public QueueEntityDTO toDTO(QueueEntity queueEntity) {
        if (queueEntity == null) {
            return null;
        }
        return QueueEntityDTO.builder()
                .id(queueEntity.getId())
                .name(queueEntity.getName())
                .type(queueEntity.getType())
                .createdAt(queueEntity.getCreatedAt())
                .updatedAt(queueEntity.getUpdatedAt())
                .build();
    }

    public QueueEntity toEntity(QueueEntityDTO queueEntityDTO) {
        if (queueEntityDTO == null) {
            return null;
        }
        return QueueEntity.builder()
                .id(queueEntityDTO.getId())
                .name(queueEntityDTO.getName())
                .type(queueEntityDTO.getType())
                .createdAt(queueEntityDTO.getCreatedAt())
                .updatedAt(queueEntityDTO.getUpdatedAt())
                .build();
    }
}