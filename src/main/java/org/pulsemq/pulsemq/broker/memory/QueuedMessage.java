package org.pulsemq.pulsemq.broker.memory;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Getter
@Builder
@AllArgsConstructor
public class QueuedMessage {

    private final UUID messageId;
    private final UUID queueId;
    private final String routingKey;
    private final String payload;
    private final Map<String, String> headers;
    private final Instant enqueuedAt;
}

