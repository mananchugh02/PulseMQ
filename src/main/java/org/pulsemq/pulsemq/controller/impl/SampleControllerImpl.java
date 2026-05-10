package org.pulsemq.pulsemq.controller.impl;

import org.pulsemq.pulsemq.controller.api.SampleControllerApi;
import org.pulsemq.pulsemq.service.SampleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sample")
public class SampleControllerImpl implements SampleControllerApi {

    private SampleControllerApi sampleApi;

    @Autowired
    private SampleService sampleService;

    @GetMapping("/hello")
    public String getHello() {
        return sampleService.getGreeting();
    }
}
