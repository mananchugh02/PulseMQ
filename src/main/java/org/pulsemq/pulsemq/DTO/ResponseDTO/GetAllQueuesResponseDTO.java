package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetAllQueuesResponseDTO {

    private List<CreateQueueResponseDTO> queues;
    private int total;
}

