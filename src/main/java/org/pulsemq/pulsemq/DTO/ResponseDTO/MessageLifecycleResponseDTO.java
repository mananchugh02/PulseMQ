package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.pulsemq.pulsemq.common.enums.MessageStatus;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageLifecycleResponseDTO {

    private UUID queueId;
    private String queueName;
    private UUID messageId;
    private String action;
    private MessageStatus status;
    private Integer retryCount;
    private boolean deadLettered;
    private int readyQueueSize;
    private int deadLetterQueueSize;
    private Instant processedAt;
}

