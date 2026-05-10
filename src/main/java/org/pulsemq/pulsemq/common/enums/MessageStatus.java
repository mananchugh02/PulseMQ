package org.pulsemq.pulsemq.common.enums;

public enum MessageStatus {
    READY,
    IN_FLIGHT,
    ACKED,
    FAILED,
    DLQ,
    PURGED
}