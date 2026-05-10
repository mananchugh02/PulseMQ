package org.pulsemq.pulsemq.psql.service;

import jakarta.persistence.EntityNotFoundException;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.dto.MessageEntityDTO;
import org.pulsemq.pulsemq.psql.mapper.MessageEntityMapper;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service("psqlMessageServiceImpl")
public class MessageServiceImpl {

    @Autowired
    private MessageRepository messageRepository;
    @Autowired
    private QueueRepository queueRepository;
    @Autowired
    private MessageEntityMapper messageEntityMapper;


    public MessageEntityDTO createMessage(MessageEntityDTO messageEntityDTO) {
        try {
            MessageEntity messageEntity = prepareMessageEntity(messageEntityDTO);
            return messageEntityMapper.toDTO(messageRepository.createMessage(messageEntity));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create message", e);
        }
    }


    public MessageEntityDTO updateMessage(MessageEntityDTO messageEntityDTO) {
        try {
            MessageEntity messageEntity = prepareMessageEntity(messageEntityDTO);
            return messageEntityMapper.toDTO(messageRepository.updateMessage(messageEntity));
        } catch (Exception e) {
            throw new RuntimeException("Failed to update message", e);
        }
    }


    public MessageEntityDTO getMessageById(UUID messageId) {
        try {
            return messageRepository.getMessageById(messageId)
                    .map(messageEntityMapper::toDTO)
                    .orElseThrow(() -> new EntityNotFoundException("Message not found for id: " + messageId));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get message by id: " + messageId, e);
        }
    }


    public List<MessageEntityDTO> getMessagesByQueueId(UUID queueId) {
        try {
            return messageRepository.findAllByQueue_IdAndStatusOrderByCreatedAtAsc(queueId, MessageStatus.READY).stream()
                    .map(messageEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get messages by queue id: " + queueId, e);
        }
    }


    public List<MessageEntityDTO> getMessagesByStatus(MessageStatus messageStatus) {
        try {
            return messageRepository.findAllByStatus(messageStatus).stream()
                    .map(messageEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get messages by status: " + messageStatus, e);
        }
    }


    public List<MessageEntityDTO> getVisibleMessagesByQueueId(UUID queueId, Instant visibleAt) {
        try {
            return messageRepository.findAllByQueue_IdAndStatusAndVisibleAtLessThanEqual(queueId, MessageStatus.READY, visibleAt).stream()
                    .map(messageEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get visible messages by queue id: " + queueId, e);
        }
    }


    public List<MessageEntityDTO> getMessagesByPayloadQuery(String query) {
        try {
            return messageRepository.findAllByPayloadContainingIgnoreCase(query).stream()
                    .map(messageEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get messages by payload query: " + query, e);
        }
    }


    public void deleteMessage(MessageEntityDTO messageEntityDTO) {
        try {
            if (messageEntityDTO == null || messageEntityDTO.getId() == null) {
                throw new IllegalArgumentException("Message DTO and id must not be null");
            }
            messageRepository.deleteMessageById(messageEntityDTO.getId());
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete message", e);
        }
    }

    private MessageEntity prepareMessageEntity(MessageEntityDTO messageEntityDTO) {
        try {
            MessageEntity messageEntity = messageEntityMapper.toEntity(messageEntityDTO);
            if (messageEntity == null) {
                throw new IllegalArgumentException("Message DTO must not be null");
            }

            if (messageEntity.getQueue() == null || messageEntity.getQueue().getId() == null) {
                throw new IllegalArgumentException("Message queueId must not be null");
            }

            QueueEntity queueEntity = queueRepository.getQueueById(messageEntity.getQueue().getId())
                    .orElseThrow(() -> new EntityNotFoundException("Queue not found for id: " + messageEntity.getQueue().getId()));
            messageEntity.setQueue(queueEntity);
            return messageEntity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare message entity", e);
        }
    }
}


