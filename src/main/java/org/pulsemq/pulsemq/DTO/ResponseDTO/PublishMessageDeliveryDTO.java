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
public class PublishMessageDeliveryDTO {

    private UUID queueId;
    private String queueName;
    private UUID messageId;
    private MessageStatus status;
    private Instant visibleAt;
}

