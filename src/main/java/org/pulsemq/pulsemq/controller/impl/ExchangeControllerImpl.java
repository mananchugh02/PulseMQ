package org.pulsemq.pulsemq.controller.impl;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetAllExchangesRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllExchangesResponseDTO;
import org.pulsemq.pulsemq.common.enums.ExchangeType;
import org.pulsemq.pulsemq.controller.api.ExchangeControllerApi;
import org.pulsemq.pulsemq.service.ExchangeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/exchanges")
public class ExchangeControllerImpl implements ExchangeControllerApi {

    @Autowired
    private ExchangeService exchangeService;

    @Override
    @PostMapping("/createExchange")
    public CreateExchangeResponseDTO createExchange(@Valid @RequestBody CreateExchangeRequestDTO createExchangeRequest) {
        log.info("Incoming request - POST /api/exchanges/createExchange with exchange name: {}", createExchangeRequest.getName());
        try {
            CreateExchangeResponseDTO response = exchangeService.createExchange(createExchangeRequest);
            log.info("Successfully created exchange with ID: {} and name: {}", response.getId(), response.getName());
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid exchange creation request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error creating exchange with name: {}", createExchangeRequest.getName(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create exchange", ex);
        }
    }

    @Override
    @DeleteMapping("/deleteExchange")
    public DeleteExchangeResponseDTO deleteExchange(@Valid @RequestBody DeleteExchangeRequestDTO deleteExchangeRequest) {
        log.info("Incoming request - DELETE /api/exchanges/deleteExchange with exchange ID: {}", deleteExchangeRequest.getExchangeId());
        try {
            DeleteExchangeResponseDTO response = exchangeService.deleteExchange(deleteExchangeRequest);
            log.info("Successfully deleted exchange with ID: {}", deleteExchangeRequest.getExchangeId());
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid exchange deletion request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error deleting exchange with ID: {}", deleteExchangeRequest.getExchangeId(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete exchange", ex);
        }
    }

    @Override
    @GetMapping("/getExchanges")
    public GetAllExchangesResponseDTO getAllExchanges(
            @RequestParam(required = false) String name,
            @RequestParam(required = false) ExchangeType type) {
        log.info("Incoming request - GET /api/exchanges/getExchanges with filters: name={}, type={}", name, type);
        try {
            GetAllExchangesRequestDTO getAllExchangesRequest = GetAllExchangesRequestDTO.builder()
                    .name(name)
                    .type(type)
                    .build();
            GetAllExchangesResponseDTO response = exchangeService.getAllExchanges(getAllExchangesRequest);
            log.info("Successfully retrieved {} exchanges", response.getTotal());
            return response;
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid exchange retrieval request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error retrieving exchanges with filters: name={}, type={}", name, type, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get exchanges", ex);
        }
    }
}

