package org.pulsemq.pulsemq.psql.repository;

import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
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

    Optional<MessageEntity> findByIdAndQueue_Id(UUID messageId, UUID queueId);


    default void deleteMessageById(UUID messageId) {
        deleteById(messageId);
    }

    List<MessageEntity> findAllByQueue_Id(UUID queueId);

    List<MessageEntity> findAllByQueue_IdAndStatusOrderByCreatedAtAsc(UUID queueId, MessageStatus status);

    @Modifying
    @Query("""
            update MessageEntity m
            set m.status = :newStatus
            where m.queue.id = :queueId
              and m.status = :currentStatus
            """)
    int updateStatusByQueue_IdAndStatus(@Param("queueId") UUID queueId,
                                        @Param("currentStatus") MessageStatus currentStatus,
                                        @Param("newStatus") MessageStatus newStatus);

    List<MessageEntity> findAllByStatus(MessageStatus status);

    List<MessageEntity> findAllByPayloadContainingIgnoreCase(String payload);

    List<MessageEntity> findAllByQueue_IdAndStatusAndVisibleAtLessThanEqual(
            UUID queueId,
            MessageStatus status,
            Instant visibleAt
    );
}

