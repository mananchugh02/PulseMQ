package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateBindingResponseDTO {

    private UUID id;
    private UUID exchangeId;
    private UUID queueId;
    private String routingKey;
    private Instant createdAt;
}

