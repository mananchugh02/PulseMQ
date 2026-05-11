package org.pulsemq.pulsemq.service.wal;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class BrokerRecoveryState {

    private final AtomicBoolean recovered = new AtomicBoolean(false);

    public boolean isRecovered() {
        return recovered.get();
    }

    public void markRecovered() {
        recovered.set(true);
    }

    public void reset() {
        recovered.set(false);
    }
}

