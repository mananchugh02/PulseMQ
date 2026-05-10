package org.pulsemq.pulsemq.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.service.SampleService;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SampleServiceImpl implements SampleService {

    @Override
    public String getGreeting() {
        log.debug("Service: Generating greeting message");
        try {
            String greeting = "Hello from PulseMQ!";
            log.info("Service: Successfully generated greeting: {}", greeting);
            return greeting;
        } catch (Exception ex) {
            log.error("Service: Exception while generating greeting", ex);
            throw new RuntimeException("Failed to generate greeting", ex);
        }
    }
}
