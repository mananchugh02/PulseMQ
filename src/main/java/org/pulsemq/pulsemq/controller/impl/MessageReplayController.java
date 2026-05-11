package org.pulsemq.pulsemq.controller.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.ResponseDTO.MessageLifecycleResponseDTO;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/messages")
public class MessageReplayController {

    private final MessageRepository messageRepository;
    private final QueueRepository queueRepository;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;

    @PostMapping("/{messageId}/replay")
    public MessageLifecycleResponseDTO replayMessage(@PathVariable UUID messageId) {
        try {
            MessageEntity message = messageRepository.getMessageById(messageId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Message not found"));

            if (message.getStatus() != MessageStatus.DLQ) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only DLQ messages can be replayed");
            }

            UUID originalQueueId = message.getOriginalQueueId();
            if (originalQueueId == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Original queue information missing for replay");
            }

            QueueEntity originalQueue = queueRepository.getQueueById(originalQueueId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Original queue not found"));

            // move message back to original queue
            message.setQueue(originalQueue);
            message.setRetryCount(0);
            message.setStatus(MessageStatus.READY);
            message.setVisibleAt(Instant.now());

            MessageEntity saved = messageRepository.save(message);

            InMemoryQueue runtime = inMemoryQueueRegistry.getQueue(originalQueueId)
                    .orElseGet(() -> inMemoryQueueRegistry.registerQueue(originalQueue));

            // enqueue runtime message
            org.pulsemq.pulsemq.broker.memory.QueuedMessage queued = org.pulsemq.pulsemq.broker.memory.QueuedMessage.builder()
                    .messageId(saved.getId())
                    .queueId(originalQueueId)
                    .payload(saved.getPayload())
                    .headers(java.util.Map.of())
                    .enqueuedAt(Instant.now())
                    .build();

            boolean offered = runtime.enqueue(queued);
            log.info("Replayed message {} back to queue {} requeued={}", messageId, originalQueueId, offered);

            return MessageLifecycleResponseDTO.builder()
                    .queueId(originalQueue.getId())
                    .queueName(originalQueue.getName())
                    .messageId(saved.getId())
                    .action("REPLAY")
                    .status(saved.getStatus())
                    .retryCount(saved.getRetryCount())
                    .deadLettered(false)
                    .readyQueueSize(runtime.size())
                    .deadLetterQueueSize(0)
                    .processedAt(Instant.now())
                    .build();
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error replaying message {}", messageId, e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to replay message", e);
        }
    }
}

