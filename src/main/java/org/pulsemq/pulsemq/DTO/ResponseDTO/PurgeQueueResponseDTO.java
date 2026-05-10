package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PurgeQueueResponseDTO {

    private UUID queueId;
    private String queueName;
    private long databaseMessagesPurged;
    private int inMemoryMessagesCleared;
    private String status;
    private Instant purgedAt;
}

