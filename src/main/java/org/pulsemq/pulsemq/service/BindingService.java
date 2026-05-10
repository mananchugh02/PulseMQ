package org.pulsemq.pulsemq.service;

import org.pulsemq.pulsemq.DTO.RequestDTO.CreateBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetBindingsRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetBindingsResponseDTO;
import org.springframework.stereotype.Component;

@Component
public interface BindingService {

    CreateBindingResponseDTO createBinding(CreateBindingRequestDTO createBindingRequest);

    GetBindingsResponseDTO getBindings(GetBindingsRequestDTO getBindingsRequest);

    DeleteBindingResponseDTO deleteBinding(DeleteBindingRequestDTO deleteBindingRequest);
}

