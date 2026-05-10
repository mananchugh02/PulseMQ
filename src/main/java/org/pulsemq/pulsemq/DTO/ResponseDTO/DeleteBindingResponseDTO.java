package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeleteBindingResponseDTO {

    private UUID bindingId;
    private String status;
}

