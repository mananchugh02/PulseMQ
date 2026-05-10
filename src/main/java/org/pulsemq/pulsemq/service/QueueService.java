package org.pulsemq.pulsemq.service;

import org.pulsemq.pulsemq.DTO.RequestDTO.CreateQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetAllQueuesRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetQueueMessagesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllQueuesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PurgeQueueResponseDTO;
import org.springframework.stereotype.Component;

@Component
public interface QueueService {

    /**
     * Create a new queue
     * @param createQueueRequest the queue request data
     * @return the created queue response
     */
    CreateQueueResponseDTO createQueue(CreateQueueRequestDTO createQueueRequest);

    /**
     * Delete a queue by ID
     * @param deleteQueueRequest the queue delete request data
     * @return a delete acknowledgement response
     */
    DeleteQueueResponseDTO deleteQueue(DeleteQueueRequestDTO deleteQueueRequest);

    /**
     * Get all messages currently associated with a queue
     * @param queueId the queue identifier
     * @return queue message listing response
     */
    GetQueueMessagesResponseDTO getQueueMessages(java.util.UUID queueId);

    /**
     * Purge all messages from a queue
     * @param queueId the queue identifier
     * @return purge acknowledgement response
     */
    PurgeQueueResponseDTO purgeQueue(java.util.UUID queueId);

    /**
     * Get all queues with optional filters
     * @param getAllQueuesRequest optional filters for queue listing
     * @return list of queues matching the criteria
     */
    GetAllQueuesResponseDTO getAllQueues(GetAllQueuesRequestDTO getAllQueuesRequest);
}


