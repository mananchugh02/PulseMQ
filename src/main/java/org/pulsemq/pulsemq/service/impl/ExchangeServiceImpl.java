package org.pulsemq.pulsemq.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetAllExchangesRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllExchangesResponseDTO;
import org.pulsemq.pulsemq.psql.dto.ExchangeEntityDTO;
import org.pulsemq.pulsemq.psql.mapper.ExchangeEntityMapper;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.pulsemq.pulsemq.psql.repository.ExchangeRepository;
import org.pulsemq.pulsemq.service.ExchangeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ExchangeServiceImpl implements ExchangeService {

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    private ExchangeEntityMapper exchangeEntityMapper;

    @Override
    public CreateExchangeResponseDTO createExchange(CreateExchangeRequestDTO createExchangeRequest) {
        log.debug("Service: Creating exchange with name: {}", createExchangeRequest.getName());
        try {
            // Create a new ExchangeEntity from the request
            ExchangeEntity exchangeEntity = ExchangeEntity.builder()
                    .name(createExchangeRequest.getName())
                    .type(createExchangeRequest.getType())
                    .build();
            log.debug("Service: Built ExchangeEntity with name: {} and type: {}", exchangeEntity.getName(), exchangeEntity.getType());

            // Save to database
            ExchangeEntity savedEntity = exchangeRepository.createExchange(exchangeEntity);
            log.debug("Service: Successfully saved exchange with ID: {} to database", savedEntity.getId());

            // Convert to response DTO
            return mapToResponseDTO(exchangeEntityMapper.toDTO(savedEntity));
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while creating exchange: {}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while creating exchange with name: {}", createExchangeRequest.getName(), ex);
            throw new RuntimeException("Failed to create exchange", ex);
        }
    }

    @Override
    public DeleteExchangeResponseDTO deleteExchange(DeleteExchangeRequestDTO deleteExchangeRequest) {
        UUID exchangeId = deleteExchangeRequest.getExchangeId();
        log.debug("Service: Deleting exchange with ID: {}", exchangeId);
        try {
            // Check if exchange exists
            Optional<ExchangeEntity> exchangeEntity = exchangeRepository.getExchangeById(exchangeId);
            if (exchangeEntity.isPresent()) {
                log.debug("Service: Exchange found for ID: {}, proceeding with deletion", exchangeId);
                exchangeRepository.deleteExchangeById(exchangeId);
                log.info("Service: Successfully deleted exchange with ID: {}", exchangeId);
                return DeleteExchangeResponseDTO.builder()
                        .exchangeId(exchangeId)
                        .status("DELETED")
                        .build();
            }
            log.warn("Service: Exchange not found for deletion with ID: {}", exchangeId);
            throw new IllegalArgumentException("Exchange with ID " + exchangeId + " not found");
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while deleting exchange with ID: {}: {}", exchangeId, ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while deleting exchange with ID: {}", exchangeId, ex);
            throw new RuntimeException("Failed to delete exchange", ex);
        }
    }

    @Override
    public GetAllExchangesResponseDTO getAllExchanges(GetAllExchangesRequestDTO getAllExchangesRequest) {
        log.debug("Service: Fetching exchanges with filters - name: {}, type: {}", getAllExchangesRequest.getName(), getAllExchangesRequest.getType());
        try {
            List<ExchangeEntity> exchanges;
            String name = getAllExchangesRequest.getName();
            var type = getAllExchangesRequest.getType();

            // Apply filters based on provided parameters
            if (name != null && type != null) {
                // Filter by both name and type
                log.debug("Service: Applying filter by both name and type");
                List<ExchangeEntity> byName = exchangeRepository.findAllByNameContainingIgnoreCase(name);
                exchanges = byName.stream()
                        .filter(e -> e.getType() == type)
                        .collect(Collectors.toList());
            } else if (name != null) {
                // Filter by name only
                log.debug("Service: Applying filter by name: {}", name);
                exchanges = exchangeRepository.findAllByNameContainingIgnoreCase(name);
            } else if (type != null) {
                // Filter by type only
                log.debug("Service: Applying filter by type: {}", type);
                exchanges = exchangeRepository.findAllByType(type);
            } else {
                // No filters, get all exchanges
                log.debug("Service: Fetching all exchanges without filters");
                exchanges = exchangeRepository.findAll();
            }

            // Convert to response DTOs
            List<CreateExchangeResponseDTO> responseList = exchanges.stream()
                    .map(exchangeEntityMapper::toDTO)
                    .map(this::mapToResponseDTO)
                    .collect(Collectors.toList());

            log.info("Service: Retrieved {} exchanges from database", responseList.size());
            return GetAllExchangesResponseDTO.builder()
                    .exchanges(responseList)
                    .total(responseList.size())
                    .build();
        } catch (IllegalArgumentException ex) {
            log.error("Service: IllegalArgumentException while fetching exchanges: {}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            log.error("Service: Exception while fetching exchanges with filters - name: {}, type: {}", 
                getAllExchangesRequest.getName(), getAllExchangesRequest.getType(), ex);
            throw new RuntimeException("Failed to fetch exchanges", ex);
        }
    }

    /**
     * Helper method to map ExchangeEntityDTO to CreateExchangeResponseDTO
     */
    private CreateExchangeResponseDTO mapToResponseDTO(ExchangeEntityDTO entityDTO) {
        return CreateExchangeResponseDTO.builder()
                .id(entityDTO.getId())
                .name(entityDTO.getName())
                .type(entityDTO.getType())
                .createdAt(entityDTO.getCreatedAt())
                .updatedAt(entityDTO.getUpdatedAt())
                .build();
    }
}

