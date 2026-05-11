package org.pulsemq.pulsemq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class PulseMqApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulseMqApplication.class, args);
    }

}
