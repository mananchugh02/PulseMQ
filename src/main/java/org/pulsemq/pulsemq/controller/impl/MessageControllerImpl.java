package org.pulsemq.pulsemq.controller.impl;

import jakarta.validation.Valid;
import jakarta.persistence.EntityNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.PublishMessageRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.MessageLifecycleResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageResponseDTO;
import org.pulsemq.pulsemq.controller.api.MessageControllerApi;
import org.pulsemq.pulsemq.broker.publish.MessagePublishService;
import org.pulsemq.pulsemq.psql.service.MessageLifecycleService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/messages")
public class MessageControllerImpl implements MessageControllerApi {

    private final MessagePublishService messagePublishService;
    private final MessageLifecycleService messageLifecycleService;

    public MessageControllerImpl(MessagePublishService messagePublishService,
                                 MessageLifecycleService messageLifecycleService) {
        this.messagePublishService = messagePublishService;
        this.messageLifecycleService = messageLifecycleService;
    }

    @Override
    @PostMapping("/publish")
    public PublishMessageResponseDTO publishMessage(@Valid @RequestBody PublishMessageRequestDTO publishMessageRequest) {
        log.info("Incoming request - POST /api/messages/publish for exchange {}", publishMessageRequest.getExchangeId());
        try {
            PublishMessageResponseDTO response = messagePublishService.publishMessage(publishMessageRequest);
            log.info("Successfully published message to {} queue(s)", response.getMatchedQueues());
            return response;
        } catch (EntityNotFoundException ex) {
            log.warn("Exchange not found while publishing message: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request while publishing message: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error publishing message for exchange {}", publishMessageRequest.getExchangeId(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to publish message", ex);
        }
    }

    @Override
    @PostMapping("/{queueId}/{messageId}/ack")
    public MessageLifecycleResponseDTO ackMessage(@PathVariable UUID queueId,
                                                  @PathVariable UUID messageId) {
        log.info("Incoming request - POST /api/messages/{}/{}/ack", queueId, messageId);
        try {
            MessageLifecycleResponseDTO response = messageLifecycleService.ackMessage(queueId, messageId);
            log.info("Successfully acknowledged message {} in queue {}", messageId, queueId);
            return response;
        } catch (EntityNotFoundException ex) {
            log.warn("Message acknowledgement target not found: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request while acknowledging message {} in queue {}: {}", messageId, queueId, ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (IllegalStateException ex) {
            log.warn("Message {} in queue {} is not ready for acknowledgement: {}", messageId, queueId, ex.getMessage());
            throw new ResponseStatusException(HttpStatus.CONFLICT, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error acknowledging message {} in queue {}", messageId, queueId, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to acknowledge message", ex);
        }
    }

    @Override
    @PostMapping("/{queueId}/{messageId}/nack")
    public MessageLifecycleResponseDTO nackMessage(@PathVariable UUID queueId,
                                                  @PathVariable UUID messageId) {
        log.info("Incoming request - POST /api/messages/{}/{}/nack", queueId, messageId);
        try {
            MessageLifecycleResponseDTO response = messageLifecycleService.nackMessage(queueId, messageId);
            log.info("Successfully negatively acknowledged message {} in queue {}", messageId, queueId);
            return response;
        } catch (EntityNotFoundException ex) {
            log.warn("Message nack target not found: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request while nacking message {} in queue {}: {}", messageId, queueId, ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (IllegalStateException ex) {
            log.warn("Message {} in queue {} is not ready for nack: {}", messageId, queueId, ex.getMessage());
            throw new ResponseStatusException(HttpStatus.CONFLICT, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error nacking message {} in queue {}", messageId, queueId, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to nack message", ex);
        }
    }
}

