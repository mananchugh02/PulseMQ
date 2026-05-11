package org.pulsemq.pulsemq.service.wal;

public enum WalEventType {
    PUBLISH,
    CLAIM,
    ACK,
    NACK,
    RETRY,
    DLQ_MOVE,
    TIMEOUT_REQUEUE
}

