package org.pulsemq.pulsemq.controller.impl;

import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.CreateBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteBindingRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetBindingsRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteBindingResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetBindingsResponseDTO;
import org.pulsemq.pulsemq.controller.api.BindingControllerApi;
import org.pulsemq.pulsemq.service.BindingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/bindings")
public class BindingControllerImpl implements BindingControllerApi {

    @Autowired
    private BindingService bindingService;

    @Override
    @PostMapping("/createBinding")
    public CreateBindingResponseDTO createBinding(@Valid @RequestBody CreateBindingRequestDTO createBindingRequest) {
        log.info("Incoming request - POST /api/bindings/createBinding for exchange ID: {}, queue ID: {}, routing key: {}",
            createBindingRequest.getExchangeId(), createBindingRequest.getQueueId(), createBindingRequest.getRoutingKey());
        try {
            CreateBindingResponseDTO response = bindingService.createBinding(createBindingRequest);
            log.info("Successfully created binding with ID: {} for exchange: {}, queue: {}",
                response.getId(), response.getExchangeId(), response.getQueueId());
            return response;
        } catch (EntityNotFoundException ex) {
            log.warn("Not found - Invalid binding creation request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid binding creation request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error creating binding for exchange: {}, queue: {}",
                createBindingRequest.getExchangeId(), createBindingRequest.getQueueId(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create binding", ex);
        }
    }

    @Override
    @GetMapping("/getBindings")
    public GetBindingsResponseDTO getBindings(@RequestParam UUID exchangeId) {
        log.info("Incoming request - GET /api/bindings/getBindings for exchange ID: {}", exchangeId);
        try {
            GetBindingsRequestDTO getBindingsRequest = GetBindingsRequestDTO.builder()
                    .exchangeId(exchangeId)
                    .build();
            GetBindingsResponseDTO response = bindingService.getBindings(getBindingsRequest);
            log.info("Successfully retrieved {} bindings for exchange: {}", response.getTotal(), exchangeId);
            return response;
        } catch (EntityNotFoundException ex) {
            log.warn("Not found - Exchange not found: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid binding retrieval request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error retrieving bindings for exchange: {}", exchangeId, ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to get bindings", ex);
        }
    }

    @Override
    @DeleteMapping("/deleteBinding")
    public DeleteBindingResponseDTO deleteBinding(@Valid @RequestBody DeleteBindingRequestDTO deleteBindingRequest) {
        log.info("Incoming request - DELETE /api/bindings/deleteBinding with binding ID: {}", deleteBindingRequest.getBindingId());
        try {
            DeleteBindingResponseDTO response = bindingService.deleteBinding(deleteBindingRequest);
            log.info("Successfully deleted binding with ID: {}", deleteBindingRequest.getBindingId());
            return response;
        } catch (EntityNotFoundException ex) {
            log.warn("Not found - Binding not found: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
        } catch (IllegalArgumentException ex) {
            log.warn("Bad request - Invalid binding deletion request: {}", ex.getMessage());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            log.error("Error deleting binding with ID: {}", deleteBindingRequest.getBindingId(), ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete binding", ex);
        }
    }
}

