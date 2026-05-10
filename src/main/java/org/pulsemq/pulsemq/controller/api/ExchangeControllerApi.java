package org.pulsemq.pulsemq.controller.api;

import org.pulsemq.pulsemq.DTO.RequestDTO.CreateExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllExchangesResponseDTO;
import org.pulsemq.pulsemq.common.enums.ExchangeType;
import org.springframework.stereotype.Component;

@Component
public interface ExchangeControllerApi {

    /**
     * Create a new exchange
     * @param createExchangeRequest the exchange request data
     * @return the created exchange response
     */
    CreateExchangeResponseDTO createExchange(CreateExchangeRequestDTO createExchangeRequest);

    /**
     * Delete an exchange by ID
     * @param deleteExchangeRequest the exchange delete request data
     */
    DeleteExchangeResponseDTO deleteExchange(DeleteExchangeRequestDTO deleteExchangeRequest);

    /**
     * Get all exchanges with optional filters
     * @param name optional filter by exchange name
     * @param type optional filter by exchange type
     * @return list of exchanges matching the criteria
     */
    GetAllExchangesResponseDTO getAllExchanges(String name, ExchangeType type);
}

