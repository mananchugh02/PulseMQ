package org.pulsemq.pulsemq.DTO.ResponseDTO;

import lombok.*;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetQueueMessagesResponseDTO {

    private UUID queueId;
    private String queueName;
    private int total;
    private List<QueueMessageResponseDTO> messages;
}

