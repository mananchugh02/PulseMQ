package org.pulsemq.pulsemq.DTO;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.QueueType;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueResponseDTO {

    private UUID id;
    private String name;
    private QueueType type;
    private Instant createdAt;
    private Instant updatedAt;
}

