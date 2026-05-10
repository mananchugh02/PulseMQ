package org.pulsemq.pulsemq.DTO.RequestDTO;

import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeleteBindingRequestDTO {

    @NotNull
    private UUID bindingId;
}

