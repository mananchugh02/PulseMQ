package org.pulsemq.pulsemq.service.wal;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
@RequiredArgsConstructor
public class WalEventRecorder {

    private final WalWriterService walWriterService;

    public void record(WalEvent event) {
        if (event == null) {
            return;
        }

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    walWriterService.appendEvent(event);
                }
            });
            return;
        }

        walWriterService.appendEvent(event);
    }
}

