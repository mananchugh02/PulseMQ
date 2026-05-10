package org.pulsemq.pulsemq.DTO.RequestDTO;

import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetBindingsRequestDTO {

    @NotNull
    private UUID exchangeId;
}

