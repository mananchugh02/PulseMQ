package org.pulsemq.pulsemq.controller.impl;

import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.controller.api.SampleControllerApi;
import org.pulsemq.pulsemq.service.SampleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/sample")
public class SampleControllerImpl implements SampleControllerApi {

    @Autowired
    private SampleService sampleService;

    @GetMapping("/hello")
    public String getHello() {
        log.info("Incoming request - GET /api/sample/hello");
        try {
            String greeting = sampleService.getGreeting();
            log.info("Successfully retrieved greeting: {}", greeting);
            return greeting;
        } catch (Exception ex) {
            log.error("Error fetching greeting", ex);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to fetch greeting", ex);
        }
    }
}
