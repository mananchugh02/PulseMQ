package org.pulsemq.pulsemq.psql.service;

import jakarta.persistence.EntityNotFoundException;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.psql.dto.QueueEntityDTO;
import org.pulsemq.pulsemq.psql.mapper.QueueEntityMapper;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service("psqlQueueServiceImpl")
public class QueueServiceImpl {

    @Autowired
    private QueueRepository queueRepository;
    @Autowired
    private QueueEntityMapper queueEntityMapper;
    @Autowired
    private InMemoryQueueRegistry inMemoryQueueRegistry;
    @Autowired
    private org.pulsemq.pulsemq.service.QueueMetricsService queueMetricsService;


     
    public QueueEntityDTO createQueue(QueueEntityDTO queueEntityDTO) {
        try {
            QueueEntity queueEntity = queueEntityMapper.toEntity(queueEntityDTO);
            QueueEntity savedQueueEntity = queueRepository.createQueue(queueEntity);
            if (savedQueueEntity.getType() == org.pulsemq.pulsemq.common.enums.QueueType.MAIN) {
                // create DLQ
                String dlqName = savedQueueEntity.getName() + ".dlq";
                QueueEntity dlq = QueueEntity.builder()
                        .name(dlqName)
                        .type(org.pulsemq.pulsemq.common.enums.QueueType.DLQ)
                        .build();
                QueueEntity savedDlq = queueRepository.createQueue(dlq);
                savedQueueEntity.setDeadLetterQueue(savedDlq);
                savedQueueEntity = queueRepository.updateQueue(savedQueueEntity);
                inMemoryQueueRegistry.registerQueue(savedQueueEntity);
                inMemoryQueueRegistry.registerQueue(savedDlq);
                queueMetricsService.registerQueueMetrics(savedQueueEntity);
                queueMetricsService.registerQueueMetrics(savedDlq);
            } else {
                inMemoryQueueRegistry.registerQueue(savedQueueEntity);
                queueMetricsService.registerQueueMetrics(savedQueueEntity);
            }
            return queueEntityMapper.toDTO(savedQueueEntity);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create queue", e);
        }
    }

     
    public QueueEntityDTO updateQueue(QueueEntityDTO queueEntityDTO) {
        try {
            QueueEntity queueEntity = queueEntityMapper.toEntity(queueEntityDTO);
            QueueEntity savedQueueEntity = queueRepository.updateQueue(queueEntity);
            inMemoryQueueRegistry.registerQueue(savedQueueEntity);
            return queueEntityMapper.toDTO(savedQueueEntity);
        } catch (Exception e) {
            throw new RuntimeException("Failed to update queue", e);
        }
    }

     
    public QueueEntityDTO getQueueById(UUID queueId) {
        try {
            return queueRepository.getQueueById(queueId)
                    .map(queueEntityMapper::toDTO)
                    .orElseThrow(() -> new EntityNotFoundException("Queue not found for id: " + queueId));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queue by id: " + queueId, e);
        }
    }

     
    public QueueEntityDTO getQueueByName(String name) {
        try {
            return queueRepository.findByName(name)
                    .map(queueEntityMapper::toDTO)
                    .orElseThrow(() -> new EntityNotFoundException("Queue not found for name: " + name));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queue by name: " + name, e);
        }
    }

     
    public List<QueueEntityDTO> getQueuesByQuery(String query) {
        try {
            return queueRepository.findAllByNameContainingIgnoreCase(query).stream()
                    .map(queueEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queues by query: " + query, e);
        }
    }

     
    public void deleteQueue(QueueEntityDTO queueEntityDTO) {
        try {
            if (queueEntityDTO == null || queueEntityDTO.getId() == null) {
                throw new IllegalArgumentException("Queue DTO and id must not be null");
            }
            queueRepository.deleteQueueById(queueEntityDTO.getId());
            inMemoryQueueRegistry.removeQueue(queueEntityDTO.getId());
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete queue", e);
        }
    }
}


