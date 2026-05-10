package org.pulsemq.pulsemq.DTO.RequestDTO;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateBindingRequestDTO {

    @NotNull
    private UUID exchangeId;

    @NotNull
    private UUID queueId;

    @NotBlank
    private String routingKey;
}

