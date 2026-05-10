package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeleteQueueResponseDTO {

    private UUID queueId;
    private String status;
}

