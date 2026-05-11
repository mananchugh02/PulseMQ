package org.pulsemq.pulsemq.controller.api;

import org.pulsemq.pulsemq.DTO.RequestDTO.CreateQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllQueuesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetQueueMessagesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.QueueMessageResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PurgeQueueResponseDTO;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.springframework.stereotype.Component;
import org.springframework.http.ResponseEntity;

import java.util.UUID;

@Component
public interface QueueControllerApi {

    /**
     * Create a new queue
     * @param createQueueRequest the queue request data
     * @return the created queue response
     */
    CreateQueueResponseDTO createQueue(CreateQueueRequestDTO createQueueRequest);

    /**
     * Delete a queue by ID
     * @param deleteQueueRequest the queue delete request data
     */
    DeleteQueueResponseDTO deleteQueue(DeleteQueueRequestDTO deleteQueueRequest);

    /**
     * Get messages currently stored in a queue
     * @param queueId queue identifier
     * @return queue messages response
     */
    GetQueueMessagesResponseDTO getQueueMessages(UUID queueId);

    ResponseEntity<QueueMessageResponseDTO> consumeMessage(UUID queueId, Long timeoutSeconds);

    /**
     * Purge all messages from a queue
     * @param queueId queue identifier
     * @return purge acknowledgement response
     */
    PurgeQueueResponseDTO purgeQueue(UUID queueId);

    /**
     * Get all queues with optional filters
     * @param name optional filter by queue name
     * @param type optional filter by queue type
     * @return list of queues matching the criteria
     */
    GetAllQueuesResponseDTO getAllQueues(String name, QueueType type);
}



