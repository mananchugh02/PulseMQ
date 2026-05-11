package org.pulsemq.pulsemq.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RetryPendingScheduler {

    private final RetryService retryService;
    private final org.pulsemq.pulsemq.service.wal.BrokerRecoveryState brokerRecoveryState;

    @Scheduled(fixedDelay = 5000)
    public void run() {
        if (!brokerRecoveryState.isRecovered()) {
            return;
        }
        try {
            retryService.processPendingRetries();
        } catch (Exception e) {
            log.error("Error while running retry pending scheduler", e);
        }
    }
}

