package org.pulsemq.pulsemq.controller.api;

import org.pulsemq.pulsemq.DTO.RequestDTO.PublishMessageRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.MessageLifecycleResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageResponseDTO;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public interface MessageControllerApi {

    PublishMessageResponseDTO publishMessage(PublishMessageRequestDTO publishMessageRequest);

    MessageLifecycleResponseDTO ackMessage(UUID queueId, UUID messageId);

    MessageLifecycleResponseDTO nackMessage(UUID queueId, UUID messageId);
}

