package org.pulsemq.pulsemq.psql.repository;

import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface MessageRepository extends JpaRepository<MessageEntity, UUID> {

    default MessageEntity createMessage(MessageEntity messageEntity) {
        return save(messageEntity);
    }

    default MessageEntity updateMessage(MessageEntity messageEntity) {
        return save(messageEntity);
    }

    default Optional<MessageEntity> getMessageById(UUID messageId) {
        return findById(messageId);
    }

    default void deleteMessage(MessageEntity messageEntity) {
        delete(messageEntity);
    }

    default void deleteMessageById(UUID messageId) {
        deleteById(messageId);
    }

    List<MessageEntity> findAllByQueue_Id(UUID queueId);

    List<MessageEntity> findAllByStatus(MessageStatus status);

    List<MessageEntity> findAllByPayloadContainingIgnoreCase(String payload);

    List<MessageEntity> findAllByQueue_IdAndStatusAndVisibleAtLessThanEqual(
            UUID queueId,
            MessageStatus status,
            Instant visibleAt
    );
}

