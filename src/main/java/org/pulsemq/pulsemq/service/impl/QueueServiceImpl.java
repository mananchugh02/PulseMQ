package org.pulsemq.pulsemq.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteQueueRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetAllQueuesRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllQueuesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetQueueMessagesResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PurgeQueueResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.QueueMessageResponseDTO;
import org.pulsemq.pulsemq.psql.dto.QueueEntityDTO;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.mapper.QueueEntityMapper;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.pulsemq.pulsemq.service.QueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class QueueServiceImpl implements QueueService {

    @Autowired
    private QueueRepository queueRepository;

    @Autowired
    private QueueEntityMapper queueEntityMapper;

    @Autowired
    private InMemoryQueueRegistry inMemoryQueueRegistry;

    @Autowired
    private MessageRepository messageRepository;
    @Autowired
    private org.pulsemq.pulsemq.service.QueueMetricsService queueMetricsService;

    @Override
    public CreateQueueResponseDTO createQueue(CreateQueueRequestDTO createQueueRequest) {
        log.debug("Service: Creating queue with name: {}", createQueueRequest.getName());
        try {
            // Create a new QueueEntity from the request
            QueueEntity queueEntity = QueueEntity.builder()
                    .name(createQueueRequest.getName())
                    .type(createQueueRequest.getType())
                    .build();
            log.debug("Service: Built QueueEntity with name: {} and type: {}", queueEntity.getName(), queueEntity.getType());

            // Save to database
            QueueEntity savedEntity = queueRepository.createQueue(queueEntity);
            // If MAIN queue, automatically create a DLQ and associate it
                if (savedEntity.getType() == org.pulsemq.pulsemq.common.enums.QueueType.MAIN) {
                String dlqName = savedEntity.getName() + ".dlq";
                QueueEntity dlq = QueueEntity.builder()
                        .name(dlqName)
                        .type(org.pulsemq.pulsemq.common.enums.QueueType.DLQ)
                        .build();
                QueueEntity savedDlq = queueRepository.createQueue(dlq);
                // associate
                savedEntity.setDeadLetterQueue(savedDlq);
                savedEntity = queueRepository.updateQueue(savedEntity);
                // register both runtime queues
                inMemoryQueueRegistry.registerQueue(savedEntity);
                inMemoryQueueRegistry.registerQueue(savedDlq);
                // register metrics for both
                queueMetricsService.registerQueueMetrics(savedEntity);
                queueMetricsService.registerQueueMetrics(savedDlq);
            } else {
                inMemoryQueueRegistry.registerQueue(savedEntity);
                queueMetricsService.registerQueueMetrics(savedEntity);
            }
            log.debug("Service: Successfully saved queue with ID: {} to database", savedEntity.getId());

            // Convert to response DTO
            return mapToResponseDTO(queueEntityMapper.toDTO(savedEntity));
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while creating queue: {}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while creating queue with name: {}", createQueueRequest.getName(), ex);
            throw new RuntimeException("Failed to create queue", ex);
        }
    }

    @Override
    public DeleteQueueResponseDTO deleteQueue(DeleteQueueRequestDTO deleteQueueRequest) {
        UUID queueId = deleteQueueRequest.getQueueId();
        log.debug("Service: Deleting queue with ID: {}", queueId);
        try {
            // Check if queue exists
            Optional<QueueEntity> queueEntity = queueRepository.getQueueById(queueId);
            if (queueEntity.isPresent()) {
                log.debug("Service: Queue found for ID: {}, proceeding with deletion", queueId);
                queueRepository.deleteQueueById(queueId);
                inMemoryQueueRegistry.removeQueue(queueId);
                log.info("Service: Successfully deleted queue with ID: {}", queueId);
                return DeleteQueueResponseDTO.builder()
                        .queueId(queueId)
                        .status("DELETED")
                        .build();
            }
            log.warn("Service: Queue not found for deletion with ID: {}", queueId);
            throw new IllegalArgumentException("Queue with ID " + queueId + " not found");
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while deleting queue with ID: {}: {}", queueId, ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while deleting queue with ID: {}", queueId, ex);
            throw new RuntimeException("Failed to delete queue", ex);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public GetQueueMessagesResponseDTO getQueueMessages(UUID queueId) {
        log.debug("Service: Fetching messages for queue ID: {}", queueId);
        try {
            QueueEntity queueEntity = queueRepository.getQueueById(queueId)
                    .orElseThrow(() -> new IllegalArgumentException("Queue with ID " + queueId + " not found"));

            List<QueueMessageResponseDTO> messages = messageRepository.findAllByQueue_IdAndStatusOrderByCreatedAtAsc(queueId, MessageStatus.READY).stream()
                    .map(this::mapToQueueMessageResponseDTO)
                    .collect(Collectors.toList());

            return GetQueueMessagesResponseDTO.builder()
                    .queueId(queueId)
                    .queueName(queueEntity.getName())
                    .total(messages.size())
                    .messages(messages)
                    .build();
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while fetching queue messages for queue ID {}: {}", queueId, ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while fetching queue messages for queue ID: {}", queueId, ex);
            throw new RuntimeException("Failed to fetch queue messages", ex);
        }
    }

    @Override
    @Transactional
    public PurgeQueueResponseDTO purgeQueue(UUID queueId) {
        log.debug("Service: Purging queue ID: {}", queueId);
        try {
            QueueEntity queueEntity = queueRepository.getQueueById(queueId)
                    .orElseThrow(() -> new IllegalArgumentException("Queue with ID " + queueId + " not found"));

            int databaseMessagesPurged = 0;
            databaseMessagesPurged += messageRepository.updateStatusByQueue_IdAndStatus(queueId, MessageStatus.READY, MessageStatus.PURGED);
            databaseMessagesPurged += messageRepository.updateStatusByQueue_IdAndStatus(queueId, MessageStatus.IN_FLIGHT, MessageStatus.PURGED);
            databaseMessagesPurged += messageRepository.updateStatusByQueue_IdAndStatus(queueId, MessageStatus.RETRY_PENDING, MessageStatus.PURGED);

            int inMemoryMessagesCleared = inMemoryQueueRegistry.getQueue(queueId)
                    .map(queue -> queue.clear() + queue.clearInFlight())
                    .orElse(0);

            return PurgeQueueResponseDTO.builder()
                    .queueId(queueId)
                    .queueName(queueEntity.getName())
                    .databaseMessagesPurged(databaseMessagesPurged)
                    .inMemoryMessagesCleared(inMemoryMessagesCleared)
                    .status("PURGED")
                    .purgedAt(java.time.Instant.now())
                    .build();
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while purging queue ID {}: {}", queueId, ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while purging queue ID: {}", queueId, ex);
            throw new RuntimeException("Failed to purge queue", ex);
        }
    }

    @Override
    public GetAllQueuesResponseDTO getAllQueues(GetAllQueuesRequestDTO getAllQueuesRequest) {
        log.debug("Service: Fetching queues with filters - name: {}, type: {}", getAllQueuesRequest.getName(), getAllQueuesRequest.getType());
        try {
            List<QueueEntity> queues;
            String name = getAllQueuesRequest.getName();
            var type = getAllQueuesRequest.getType();

            // Apply filters based on provided parameters
            if (name != null && type != null) {
                // Filter by both name and type
                log.debug("Service: Applying filter by both name and type");
                List<QueueEntity> byName = queueRepository.findAllByNameContainingIgnoreCase(name);
                queues = byName.stream()
                        .filter(q -> q.getType() == type)
                        .collect(Collectors.toList());
            } else if (name != null) {
                // Filter by name only
                log.debug("Service: Applying filter by name: {}", name);
                queues = queueRepository.findAllByNameContainingIgnoreCase(name);
            } else if (type != null) {
                // Filter by type only
                log.debug("Service: Applying filter by type: {}", type);
                queues = queueRepository.findAllByType(type);
            } else {
                // No filters, get all queues
                log.debug("Service: Fetching all queues without filters");
                queues = queueRepository.findAll();
            }

            // Convert to response DTOs
            List<CreateQueueResponseDTO> responseList = queues.stream()
                    .map(queueEntityMapper::toDTO)
                    .map(this::mapToResponseDTO)
                    .collect(Collectors.toList());

            log.info("Service: Retrieved {} queues from database", responseList.size());
            return GetAllQueuesResponseDTO.builder()
                    .queues(responseList)
                    .total(responseList.size())
                    .build();
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while fetching queues: {}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while fetching queues with filters - name: {}, type: {}", 
                getAllQueuesRequest.getName(), getAllQueuesRequest.getType(), ex);
            throw new RuntimeException("Failed to fetch queues", ex);
        }
    }

    /**
     * Helper method to map QueueEntityDTO to QueueResponseDTO
     */
    private CreateQueueResponseDTO mapToResponseDTO(QueueEntityDTO entityDTO) {
        return CreateQueueResponseDTO.builder()
                .id(entityDTO.getId())
                .name(entityDTO.getName())
                .type(entityDTO.getType())
                .createdAt(entityDTO.getCreatedAt())
                .updatedAt(entityDTO.getUpdatedAt())
                .build();
    }

    private QueueMessageResponseDTO mapToQueueMessageResponseDTO(MessageEntity messageEntity) {
        return QueueMessageResponseDTO.builder()
                .id(messageEntity.getId())
                .queueId(messageEntity.getQueue() != null ? messageEntity.getQueue().getId() : null)
                .payload(messageEntity.getPayload())
                .status(messageEntity.getStatus())
                .retryCount(messageEntity.getRetryCount())
                .visibleAt(messageEntity.getVisibleAt())
                .createdAt(messageEntity.getCreatedAt())
                .updatedAt(messageEntity.getUpdatedAt())
                .build();
    }
}


