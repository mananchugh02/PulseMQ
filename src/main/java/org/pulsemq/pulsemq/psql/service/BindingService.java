package org.pulsemq.pulsemq.psql.service;

import jakarta.persistence.EntityNotFoundException;
import org.pulsemq.pulsemq.psql.dto.BindingEntityDTO;
import org.pulsemq.pulsemq.psql.mapper.BindingEntityMapper;
import org.pulsemq.pulsemq.psql.model.BindingEntity;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.BindingRepository;
import org.pulsemq.pulsemq.psql.repository.ExchangeRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class BindingService {

    @Autowired
    private BindingRepository bindingRepository;
    @Autowired
    private ExchangeRepository exchangeRepository;
    @Autowired
    private QueueRepository queueRepository;
    @Autowired
    private BindingEntityMapper bindingEntityMapper;

    public BindingEntityDTO createBinding(BindingEntityDTO bindingEntityDTO) {
        try {
            BindingEntity bindingEntity = prepareBindingEntity(bindingEntityDTO);
            return bindingEntityMapper.toDTO(bindingRepository.createBinding(bindingEntity));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create binding", e);
        }
    }

    public BindingEntityDTO updateBinding(BindingEntityDTO bindingEntityDTO) {
        try {
            BindingEntity bindingEntity = prepareBindingEntity(bindingEntityDTO);
            return bindingEntityMapper.toDTO(bindingRepository.updateBinding(bindingEntity));
        } catch (Exception e) {
            throw new RuntimeException("Failed to update binding", e);
        }
    }

    public BindingEntityDTO getBindingById(UUID bindingId) {
        try {
            return bindingRepository.getBindingById(bindingId)
                    .map(bindingEntityMapper::toDTO)
                    .orElseThrow(() -> new EntityNotFoundException("Binding not found for id: " + bindingId));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get binding by id: " + bindingId, e);
        }
    }

    public List<BindingEntityDTO> getBindingsByExchangeId(UUID exchangeId) {
        try {
            return bindingRepository.findAllByExchange_Id(exchangeId).stream()
                    .map(bindingEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get bindings by exchange id: " + exchangeId, e);
        }
    }

    public List<BindingEntityDTO> getBindingsByQueueId(UUID queueId) {
        try {
            return bindingRepository.findAllByQueue_Id(queueId).stream()
                    .map(bindingEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get bindings by queue id: " + queueId, e);
        }
    }

    public List<BindingEntityDTO> getBindingsByRoutingKey(String routingKey) {
        try {
            return bindingRepository.findAllByRoutingKeyContainingIgnoreCase(routingKey).stream()
                    .map(bindingEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get bindings by routing key: " + routingKey, e);
        }
    }

    public List<BindingEntityDTO> getBindingsByQuery(String query) {
        try {
            return bindingRepository
                    .findAllByExchange_NameContainingIgnoreCaseOrQueue_NameContainingIgnoreCaseOrRoutingKeyContainingIgnoreCase(
                            query,
                            query,
                            query
                    )
                    .stream()
                    .map(bindingEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get bindings by query: " + query, e);
        }
    }

    public void deleteBinding(BindingEntityDTO bindingEntityDTO) {
        try {
            if (bindingEntityDTO == null || bindingEntityDTO.getId() == null) {
                throw new IllegalArgumentException("Binding DTO and id must not be null");
            }
            bindingRepository.deleteBindingById(bindingEntityDTO.getId());
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete binding", e);
        }
    }

    private BindingEntity prepareBindingEntity(BindingEntityDTO bindingEntityDTO) {
        try {
            BindingEntity bindingEntity = bindingEntityMapper.toEntity(bindingEntityDTO);
            if (bindingEntity == null) {
                throw new IllegalArgumentException("Binding DTO must not be null");
            }

            if (bindingEntity.getExchange() == null || bindingEntity.getExchange().getId() == null) {
                throw new IllegalArgumentException("Binding exchangeId must not be null");
            }
            if (bindingEntity.getQueue() == null || bindingEntity.getQueue().getId() == null) {
                throw new IllegalArgumentException("Binding queueId must not be null");
            }

            ExchangeEntity exchangeEntity = exchangeRepository.getExchangeById(bindingEntity.getExchange().getId())
                    .orElseThrow(() -> new EntityNotFoundException("Exchange not found for id: " + bindingEntity.getExchange().getId()));
            QueueEntity queueEntity = queueRepository.getQueueById(bindingEntity.getQueue().getId())
                    .orElseThrow(() -> new EntityNotFoundException("Queue not found for id: " + bindingEntity.getQueue().getId()));

            bindingEntity.setExchange(exchangeEntity);
            bindingEntity.setQueue(queueEntity);
            return bindingEntity;
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare binding entity", e);
        }
    }
}


