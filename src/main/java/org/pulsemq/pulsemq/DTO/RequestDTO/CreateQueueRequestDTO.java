package org.pulsemq.pulsemq.DTO.RequestDTO;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import org.pulsemq.pulsemq.common.enums.QueueType;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateQueueRequestDTO {

    @NotBlank
    private String name;

    @NotNull
    private QueueType type;
}

