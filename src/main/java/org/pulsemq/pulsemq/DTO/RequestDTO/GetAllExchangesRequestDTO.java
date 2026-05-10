package org.pulsemq.pulsemq.DTO.RequestDTO;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.ExchangeType;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetAllExchangesRequestDTO {

    private String name;
    private ExchangeType type;
}

