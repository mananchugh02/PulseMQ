package org.pulsemq.pulsemq.service.impl;

import jakarta.persistence.EntityNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetBindingsRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetBindingsResponseDTO;
import org.pulsemq.pulsemq.psql.model.BindingEntity;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.BindingRepository;
import org.pulsemq.pulsemq.psql.repository.ExchangeRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.pulsemq.pulsemq.service.BindingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BindingServiceImpl implements BindingService {

    @Autowired
    private BindingRepository bindingRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    private QueueRepository queueRepository;

    @Override
    public CreateBindingResponseDTO createBinding(CreateBindingRequestDTO createBindingRequest) {
        UUID exchangeId = createBindingRequest.getExchangeId();
        UUID queueId = createBindingRequest.getQueueId();
        String routingKey = createBindingRequest.getRoutingKey();
        log.debug("Service: Creating binding for exchange ID: {}, queue ID: {}, routing key: {}", exchangeId, queueId, routingKey);
        try {
            ExchangeEntity exchangeEntity = exchangeRepository.getExchangeById(exchangeId)
                    .orElseThrow(() -> {
                        log.warn("Service: Exchange not found for ID: {}", exchangeId);
                        return new EntityNotFoundException("Exchange not found for id: " + exchangeId);
                    });
            QueueEntity queueEntity = queueRepository.getQueueById(queueId)
                    .orElseThrow(() -> {
                        log.warn("Service: Queue not found for ID: {}", queueId);
                        return new EntityNotFoundException("Queue not found for id: " + queueId);
                    });

            String trimmedRoutingKey = routingKey.trim();
            bindingRepository.findByExchange_IdAndQueue_IdAndRoutingKey(exchangeEntity.getId(), queueEntity.getId(), trimmedRoutingKey)
                    .ifPresent(existing -> {
                        log.warn("Service: Binding already exists for exchangeId={}, queueId={}, routingKey={}", exchangeId, queueId, trimmedRoutingKey);
                        throw new IllegalArgumentException("Binding already exists for exchangeId=" + exchangeEntity.getId()
                                + ", queueId=" + queueEntity.getId() + ", routingKey=" + trimmedRoutingKey);
                    });

            BindingEntity bindingEntity = BindingEntity.builder()
                    .exchange(exchangeEntity)
                    .queue(queueEntity)
                    .routingKey(trimmedRoutingKey)
                    .build();

            BindingEntity savedBinding = bindingRepository.save(bindingEntity);
            log.info("Service: Successfully created binding with ID: {}", savedBinding.getId());
            return toResponseDTO(savedBinding);
        } catch (IllegalArgumentException | EntityNotFoundException ex) {
            log.error("Service: Exception while creating binding: {}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while creating binding for exchange: {}, queue: {}", exchangeId, queueId, ex);
            throw new RuntimeException("Failed to create binding", ex);
        }
    }

    @Override
    public GetBindingsResponseDTO getBindings(GetBindingsRequestDTO getBindingsRequest) {
        UUID exchangeId = getBindingsRequest.getExchangeId();
        log.debug("Service: Fetching bindings for exchange ID: {}", exchangeId);
        try {
            exchangeRepository.getExchangeById(exchangeId)
                    .orElseThrow(() -> {
                        log.warn("Service: Exchange not found for ID: {}", exchangeId);
                        return new EntityNotFoundException("Exchange not found for id: " + exchangeId);
                    });

            List<CreateBindingResponseDTO> bindings = bindingRepository.findAllByExchange_Id(exchangeId).stream()
                    .map(this::toResponseDTO)
                    .collect(Collectors.toList());

            log.info("Service: Retrieved {} bindings for exchange: {}", bindings.size(), exchangeId);
            return GetBindingsResponseDTO.builder()
                    .bindings(bindings)
                    .total(bindings.size())
                    .build();
        } catch (IllegalArgumentException | EntityNotFoundException ex) {
            log.error("Service: Exception while fetching bindings: {}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while fetching bindings for exchange: {}", exchangeId, ex);
            throw new RuntimeException("Failed to fetch bindings", ex);
        }
    }

    @Override
    public DeleteBindingResponseDTO deleteBinding(DeleteBindingRequestDTO deleteBindingRequest) {
        UUID bindingId = deleteBindingRequest.getBindingId();
        log.debug("Service: Deleting binding with ID: {}", bindingId);
        try {
            BindingEntity bindingEntity = bindingRepository.getBindingById(bindingId)
                    .orElseThrow(() -> {
                        log.warn("Service: Binding not found for ID: {}", bindingId);
                        return new EntityNotFoundException("Binding not found for id: " + bindingId);
                    });

            bindingRepository.delete(bindingEntity);
            log.info("Service: Successfully deleted binding with ID: {}", bindingId);
            return DeleteBindingResponseDTO.builder()
                    .bindingId(bindingId)
                    .status("DELETED")
                    .build();
        } catch (IllegalArgumentException | EntityNotFoundException ex) {
            log.error("Service: Exception while deleting binding: {}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while deleting binding with ID: {}", bindingId, ex);
            throw new RuntimeException("Failed to delete binding", ex);
        }
    }

    private CreateBindingResponseDTO toResponseDTO(BindingEntity bindingEntity) {
        return CreateBindingResponseDTO.builder()
                .id(bindingEntity.getId())
                .exchangeId(bindingEntity.getExchange() != null ? bindingEntity.getExchange().getId() : null)
                .queueId(bindingEntity.getQueue() != null ? bindingEntity.getQueue().getId() : null)
                .routingKey(bindingEntity.getRoutingKey())
                .createdAt(bindingEntity.getCreatedAt())
                .build();
    }
}

