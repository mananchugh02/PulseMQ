package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.MessageStatus;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueMessageResponseDTO {

    private UUID id;
    private UUID queueId;
    private String payload;
    private MessageStatus status;
    private Integer retryCount;
    private Instant visibleAt;
    private Instant createdAt;
    private Instant updatedAt;
}

