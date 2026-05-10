package org.pulsemq.pulsemq.DTO.RequestDTO;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.NotBlank;
import lombok.*;

import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PublishMessageRequestDTO {

    @NotNull
    private UUID exchangeId;

    @NotBlank
    private String payload;

    private String routingKey;

    private Map<String, String> headers;
}

