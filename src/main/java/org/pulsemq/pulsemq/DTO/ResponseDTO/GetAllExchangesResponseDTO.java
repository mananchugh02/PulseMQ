package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetAllExchangesResponseDTO {

    private List<CreateExchangeResponseDTO> exchanges;
    private int total;
}

