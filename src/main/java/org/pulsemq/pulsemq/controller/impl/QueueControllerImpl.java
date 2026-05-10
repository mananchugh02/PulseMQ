package org.pulsemq.pulsemq.controller.impl;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetAllQueuesRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllQueuesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetQueueMessagesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PurgeQueueResponseDTO;
import org.pulsemq.pulsemq.common.enums.QueueType;
import org.pulsemq.pulsemq.controller.api.QueueControllerApi;
import org.pulsemq.pulsemq.service.QueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/queues")
public class QueueControllerImpl implements QueueControllerApi {

    @Autowired
    private QueueService queueService;

    @Override
    @PostMapping("/createQueue")
    public CreateQueueResponseDTO createQueue(@Valid @RequestBody CreateQueueRequestDTO createQueueRequest) {
        log.info("Incoming request - POST /api/queues/createQueue with queue name: {}", createQueueRequest.getName());
        try {
            CreateQueueResponseDTO response = queueService.createQueue(createQueueRequest);
            log.info("Successfully created queue with ID: {} and name: {}", response.getId(), response.getName());
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid queue creation request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error creating queue with name: {}", createQueueRequest.getName(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create queue", ex);
        }
    }

    @Override
    @DeleteMapping("/deleteQueue")
    public DeleteQueueResponseDTO deleteQueue(@Valid @RequestBody DeleteQueueRequestDTO deleteQueueRequest) {
        log.info("Incoming request - DELETE /api/queues/deleteQueue with queue ID: {}", deleteQueueRequest.getQueueId());
        try {
            DeleteQueueResponseDTO response = queueService.deleteQueue(deleteQueueRequest);
            log.info("Successfully deleted queue with ID: {}", deleteQueueRequest.getQueueId());
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid queue deletion request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error deleting queue with ID: {}", deleteQueueRequest.getQueueId(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete queue", ex);
        }
    }

    @Override
    @GetMapping("/getQueues")
    public GetAllQueuesResponseDTO getAllQueues(
            @RequestParam(required = false) String name,
            @RequestParam(required = false) QueueType type) {
        log.info("Incoming request - GET /api/queues/getQueues with filters: name={}, type={}", name, type);
        try {
            GetAllQueuesRequestDTO getAllQueuesRequest = GetAllQueuesRequestDTO.builder()
                    .name(name)
                    .type(type)
                    .build();
            GetAllQueuesResponseDTO response = queueService.getAllQueues(getAllQueuesRequest);
            log.info("Successfully retrieved {} queues", response.getTotal());
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid queue retrieval request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error retrieving queues with filters: name={}, type={}", name, type, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get queues", ex);
        }
    }

    @Override
    @GetMapping("/{queueId}/messages")
    public GetQueueMessagesResponseDTO getQueueMessages(@PathVariable UUID queueId) {
        log.info("Incoming request - GET /api/queues/{}/messages", queueId);
        try {
            GetQueueMessagesResponseDTO response = queueService.getQueueMessages(queueId);
            log.info("Successfully retrieved {} messages for queue {}", response.getTotal(), queueId);
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid queue message retrieval request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error retrieving messages for queue {}", queueId, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get queue messages", ex);
        }
    }

    @Override
    @DeleteMapping("/{queueId}/purge")
    public PurgeQueueResponseDTO purgeQueue(@PathVariable UUID queueId) {
        log.info("Incoming request - DELETE /api/queues/{}/purge", queueId);
        try {
            PurgeQueueResponseDTO response = queueService.purgeQueue(queueId);
            log.info("Successfully purged queue {}", queueId);
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid queue purge request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error purging queue {}", queueId, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to purge queue", ex);
        }
    }
}


