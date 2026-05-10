package org.pulsemq.pulsemq.psql.mapper;

import org.pulsemq.pulsemq.psql.dto.MessageEntityDTO;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.springframework.stereotype.Component;

@Component
public class MessageEntityMapper {

    public MessageEntityDTO toDTO(MessageEntity messageEntity) {
        if (messageEntity == null) {
            return null;
        }
        return MessageEntityDTO.builder()
                .id(messageEntity.getId())
                .queueId(messageEntity.getQueue() != null ? messageEntity.getQueue().getId() : null)
                .payload(messageEntity.getPayload())
                .status(messageEntity.getStatus())
                .retryCount(messageEntity.getRetryCount())
                .visibleAt(messageEntity.getVisibleAt())
                .createdAt(messageEntity.getCreatedAt())
                .updatedAt(messageEntity.getUpdatedAt())
                .build();
    }

    public MessageEntity toEntity(MessageEntityDTO messageEntityDTO) {
        if (messageEntityDTO == null) {
            return null;
        }

        QueueEntity queueEntity = null;
        if (messageEntityDTO.getQueueId() != null) {
            queueEntity = QueueEntity.builder().id(messageEntityDTO.getQueueId()).build();
        }

        return MessageEntity.builder()
                .id(messageEntityDTO.getId())
                .queue(queueEntity)
                .payload(messageEntityDTO.getPayload())
                .status(messageEntityDTO.getStatus())
                .retryCount(messageEntityDTO.getRetryCount())
                .visibleAt(messageEntityDTO.getVisibleAt())
                .createdAt(messageEntityDTO.getCreatedAt())
                .updatedAt(messageEntityDTO.getUpdatedAt())
                .build();
    }
}

