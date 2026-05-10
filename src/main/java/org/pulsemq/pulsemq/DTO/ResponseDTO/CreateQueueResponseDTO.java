package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.QueueType;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateQueueResponseDTO {

    private UUID id;
    private String name;
    private QueueType type;
    private Instant createdAt;
    private Instant updatedAt;
}

