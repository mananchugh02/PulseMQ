package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetBindingsResponseDTO {

    private List<CreateBindingResponseDTO> bindings;
    private int total;
}

