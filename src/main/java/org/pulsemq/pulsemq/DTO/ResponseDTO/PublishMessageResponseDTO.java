package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PublishMessageResponseDTO {

    private UUID exchangeId;
    private String exchangeName;
    private String exchangeType;
    private String routingKey;
    private int matchedQueues;
    private int savedMessages;
    private Instant publishedAt;
    private List<PublishMessageDeliveryDTO> deliveries;
}

