package org.pulsemq.pulsemq.common.enums;

public enum MessageStatus {
    READY,
    IN_FLIGHT,
    RETRY_PENDING,
    ACKED,
    FAILED,
    DLQ,
    PURGED
}