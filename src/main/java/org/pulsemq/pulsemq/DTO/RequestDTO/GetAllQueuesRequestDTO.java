package org.pulsemq.pulsemq.DTO.RequestDTO;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.QueueType;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetAllQueuesRequestDTO {

    private String name;
    private QueueType type;
}

