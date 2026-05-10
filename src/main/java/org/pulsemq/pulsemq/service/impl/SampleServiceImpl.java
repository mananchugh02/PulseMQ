package org.pulsemq.pulsemq.service.impl;

import org.pulsemq.pulsemq.service.SampleService;
import org.springframework.stereotype.Service;

@Service
public class SampleServiceImpl implements SampleService {

    @Override
    public String getGreeting() {
        return "Hello from PulseMQ!";
    }
}
