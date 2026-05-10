package org.pulsemq.pulsemq.DTO;

import lombok.*;
import org.pulsemq.pulsemq.common.enums.QueueType;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueRequestDTO {

    private String name;
    private QueueType type;
}

