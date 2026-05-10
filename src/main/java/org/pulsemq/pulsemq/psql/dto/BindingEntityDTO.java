package org.pulsemq.pulsemq.psql.dto;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BindingEntityDTO {

    private UUID id;
    private UUID exchangeId; // Representing ExchangeEntity by its ID
    private UUID queueId;    // Representing QueueEntity by its ID
    private String routingKey;
    private Instant createdAt;
}