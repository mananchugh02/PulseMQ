package org.pulsemq.pulsemq.psql.service;

import jakarta.persistence.EntityNotFoundException;
import org.pulsemq.pulsemq.psql.dto.ExchangeEntityDTO;
import org.pulsemq.pulsemq.psql.mapper.ExchangeEntityMapper;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.pulsemq.pulsemq.psql.repository.ExchangeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service("psqlExchangeServiceImpl")
public class ExchangeServiceImpl {

    @Autowired
    private ExchangeRepository exchangeRepository;
    @Autowired
    private ExchangeEntityMapper exchangeEntityMapper;

    public ExchangeEntityDTO createExchange(ExchangeEntityDTO exchangeEntityDTO) {
        try {
            ExchangeEntity exchangeEntity = exchangeEntityMapper.toEntity(exchangeEntityDTO);
            return exchangeEntityMapper.toDTO(exchangeRepository.createExchange(exchangeEntity));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create exchange", e);
        }
    }

    public ExchangeEntityDTO updateExchange(ExchangeEntityDTO exchangeEntityDTO) {
        try {
            ExchangeEntity exchangeEntity = exchangeEntityMapper.toEntity(exchangeEntityDTO);
            return exchangeEntityMapper.toDTO(exchangeRepository.updateExchange(exchangeEntity));
        } catch (Exception e) {
            throw new RuntimeException("Failed to update exchange", e);
        }
    }

    public ExchangeEntityDTO getExchangeById(UUID exchangeId) {
        try {
            return exchangeRepository.getExchangeById(exchangeId)
                    .map(exchangeEntityMapper::toDTO)
                    .orElseThrow(() -> new EntityNotFoundException("Exchange not found for id: " + exchangeId));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get exchange by id: " + exchangeId, e);
        }
    }

    public ExchangeEntityDTO getExchangeByName(String name) {
        try {
            return exchangeRepository.findByName(name)
                    .map(exchangeEntityMapper::toDTO)
                    .orElseThrow(() -> new EntityNotFoundException("Exchange not found for name: " + name));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get exchange by name: " + name, e);
        }
    }

    public List<ExchangeEntityDTO> getExchangesByQuery(String query) {
        try {
            return exchangeRepository.findAllByNameContainingIgnoreCase(query).stream()
                    .map(exchangeEntityMapper::toDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get exchanges by query: " + query, e);
        }
    }

    public void deleteExchange(ExchangeEntityDTO exchangeEntityDTO) {
        try {
            if (exchangeEntityDTO == null || exchangeEntityDTO.getId() == null) {
                throw new IllegalArgumentException("Exchange DTO and id must not be null");
            }
            exchangeRepository.deleteExchangeById(exchangeEntityDTO.getId());
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete exchange", e);
        }
    }
}


