package org.pulsemq.pulsemq.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class VisibilityTimeoutScheduler {

    private final VisibilityTimeoutService visibilityTimeoutService;
    private final org.pulsemq.pulsemq.service.wal.BrokerRecoveryState brokerRecoveryState;

    @Scheduled(fixedDelay = 5000)
    public void run() {
        if (!brokerRecoveryState.isRecovered()) {
            return;
        }
        try {
            visibilityTimeoutService.recoverTimedOutMessages();
        } catch (Exception e) {
            log.error("Error while running visibility timeout recovery", e);
        }
    }
}

