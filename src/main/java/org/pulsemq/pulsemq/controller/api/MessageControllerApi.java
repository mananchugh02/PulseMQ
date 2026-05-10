package org.pulsemq.pulsemq.controller.api;

import org.pulsemq.pulsemq.DTO.RequestDTO.PublishMessageRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageResponseDTO;
import org.springframework.stereotype.Component;

@Component
public interface MessageControllerApi {

    PublishMessageResponseDTO publishMessage(PublishMessageRequestDTO publishMessageRequest);
}

