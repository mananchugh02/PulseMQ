package org.pulsemq.pulsemq.psql.dto;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.ExchangeType;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExchangeEntityDTO {

    private UUID id;
    private String name;
    private ExchangeType type;
    private Instant createdAt;
    private Instant updatedAt;
}