package org.pulsemq.pulsemq.controller.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.ResponseDTO.QueueMessageResponseDTO;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/queues")
public class DlqControllerImpl {

    private final QueueRepository queueRepository;
    private final MessageRepository messageRepository;

    @GetMapping("/{queueId}/dlq/messages")
    public List<QueueMessageResponseDTO> getDlqMessages(@PathVariable UUID queueId) {
        try {
            QueueEntity queueEntity = queueRepository.getQueueById(queueId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Queue not found"));

            QueueEntity dlq = queueEntity.getDeadLetterQueue();
            if (dlq == null) {
                return List.of();
            }

            List<MessageEntity> messages = messageRepository.findAllByQueue_IdAndStatusOrderByCreatedAtAsc(dlq.getId(), MessageStatus.DLQ);
            return messages.stream().map(this::mapToDto).collect(Collectors.toList());
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error fetching DLQ messages for queue {}", queueId, e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to fetch DLQ messages", e);
        }
    }

    private QueueMessageResponseDTO mapToDto(MessageEntity m) {
        return QueueMessageResponseDTO.builder()
                .id(m.getId())
                .queueId(m.getQueue() != null ? m.getQueue().getId() : null)
                .payload(m.getPayload())
                .status(m.getStatus())
                .retryCount(m.getRetryCount())
                .visibleAt(m.getVisibleAt())
                .createdAt(m.getCreatedAt())
                .updatedAt(m.getUpdatedAt())
                .build();
    }
}

