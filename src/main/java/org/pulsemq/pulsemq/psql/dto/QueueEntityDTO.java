package org.pulsemq.pulsemq.psql.dto;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.QueueType;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueEntityDTO {

    private UUID id;
    private String name;
    private QueueType type;
    private Instant createdAt;
    private Instant updatedAt;
}