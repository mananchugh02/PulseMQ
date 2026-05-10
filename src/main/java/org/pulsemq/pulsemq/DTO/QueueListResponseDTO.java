package org.pulsemq.pulsemq.DTO;

import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueListResponseDTO {

    private List<QueueResponseDTO> queues;
    private int total;
}

