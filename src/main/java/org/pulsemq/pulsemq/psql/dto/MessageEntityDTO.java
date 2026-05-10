package org.pulsemq.pulsemq.psql.dto;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.MessageStatus;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageEntityDTO {

    private UUID id;
    private UUID queueId; // Representing QueueEntity by its ID
    private String payload;
    private MessageStatus status;
    private Integer retryCount;
    private Instant visibleAt;
    private Instant createdAt;
    private Instant updatedAt;
}