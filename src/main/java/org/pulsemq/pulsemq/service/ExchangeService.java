package org.pulsemq.pulsemq.service;

import org.pulsemq.pulsemq.DTO.RequestDTO.CreateExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.DeleteExchangeRequestDTO;
import org.pulsemq.pulsemq.DTO.RequestDTO.GetAllExchangesRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.CreateExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.DeleteExchangeResponseDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.GetAllExchangesResponseDTO;
import org.springframework.stereotype.Component;

@Component
public interface ExchangeService {

    /**
     * Create a new exchange
     * @param createExchangeRequest the exchange request data
     * @return the created exchange response
     */
    CreateExchangeResponseDTO createExchange(CreateExchangeRequestDTO createExchangeRequest);

    /**
     * Delete an exchange by ID
     * @param deleteExchangeRequest the exchange delete request data
     * @return a delete acknowledgement response
     */
    DeleteExchangeResponseDTO deleteExchange(DeleteExchangeRequestDTO deleteExchangeRequest);

    /**
     * Get all exchanges with optional filters
     * @param getAllExchangesRequest optional filters for exchange listing
     * @return list of exchanges matching the criteria
     */
    GetAllExchangesResponseDTO getAllExchanges(GetAllExchangesRequestDTO getAllExchangesRequest);
}

