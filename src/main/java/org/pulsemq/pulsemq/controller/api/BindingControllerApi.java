package org.pulsemq.pulsemq.controller.api;

import org.pulsemq.pulsemq.DTO.RequestDTO.CreateBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetBindingsResponseDTO;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public interface BindingControllerApi {

    CreateBindingResponseDTO createBinding(CreateBindingRequestDTO createBindingRequest);

    GetBindingsResponseDTO getBindings(UUID exchangeId);

    DeleteBindingResponseDTO deleteBinding(DeleteBindingRequestDTO deleteBindingRequest);
}


