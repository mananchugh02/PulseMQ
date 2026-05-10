package org.pulsemq.pulsemq.controller.impl;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.PublishMessageRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageResponseDTO;
import org.pulsemq.pulsemq.controller.api.MessageControllerApi;
import org.pulsemq.pulsemq.broker.publish.MessagePublishService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@Slf4j
@RestController
@RequestMapping("/api/messages")
public class MessageControllerImpl implements MessageControllerApi {

    private final MessagePublishService messagePublishService;

    public MessageControllerImpl(MessagePublishService messagePublishService) {
        this.messagePublishService = messagePublishService;
    }

    @Override
    @PostMapping("/publish")
    public PublishMessageResponseDTO publishMessage(@Valid @RequestBody PublishMessageRequestDTO publishMessageRequest) {
        log.info("Incoming request - POST /api/messages/publish for exchange {}", publishMessageRequest.getExchangeId());
        try {
            PublishMessageResponseDTO response = messagePublishService.publishMessage(publishMessageRequest);
            log.info("Successfully published message to {} queue(s)", response.getMatchedQueues());
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request while publishing message: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error publishing message for exchange {}", publishMessageRequest.getExchangeId(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to publish message", ex);
        }
    }
}

