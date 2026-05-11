package org.pulsemq.pulsemq.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class QueueMetricsService {

    private final MeterRegistry meterRegistry;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;

    private final Map<String, Counter> publishedCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> consumedCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> ackCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> nackCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> retryCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> dlqCounters = new ConcurrentHashMap<>();
    private final Map<String, Integer> lastDepth = new ConcurrentHashMap<>();
    private final Set<String> registeredGaugeQueues = ConcurrentHashMap.newKeySet();

    @PostConstruct
    public void init() {
        // register gauges for existing runtime queues
        for (InMemoryQueue q : inMemoryQueueRegistry.getAllQueues()) {
            registerQueueGauges(q.getQueueId().toString(), q.getQueueName(), q);
        }
    }

    @Scheduled(fixedDelay = 15000)
    public void monitorQueueDepths() {
        for (InMemoryQueue q : inMemoryQueueRegistry.getAllQueues()) {
            if (q == null) continue;
            String id = q.getQueueId().toString();
            int depth = q.size();
            Integer prev = lastDepth.get(id);
            if (prev == null || prev != depth) {
                log.info("Queue depth changed for {} ({}): {} -> {}", q.getQueueName(), id, prev == null ? 0 : prev, depth);
                lastDepth.put(id, depth);
            }
            // warn if DLQ grows beyond threshold
            if (q.getQueueType() != null && q.getQueueType().name().equals("DLQ") && depth > 50) {
                log.warn("DLQ {} ({}) size is high: {} messages", q.getQueueName(), id, depth);
            }
        }
    }

    public void registerQueueMetrics(QueueEntity queueEntity) {
        if (queueEntity == null || queueEntity.getId() == null) return;
        InMemoryQueue queue = inMemoryQueueRegistry.getQueue(queueEntity.getId())
                .orElseGet(() -> inMemoryQueueRegistry.registerQueue(queueEntity));
        registerQueueGauges(queueEntity.getId().toString(), queueEntity.getName(), queue);
    }

    private void registerQueueGauges(String queueId, String queueName, InMemoryQueue queue) {
        if (!registeredGaugeQueues.add(queueId)) {
            return;
        }

        String[] tags = new String[]{"queueId", queueId, "queueName", queueName};

        Gauge.builder("pulsemq_queue_depth", queue, InMemoryQueue::size)
                .description("Number of ready messages in queue")
                .tags(tags)
                .register(meterRegistry);

        Gauge.builder("pulsemq_inflight_count", queue, InMemoryQueue::inFlightSize)
                .description("Number of in-flight messages in queue")
                .tags(tags)
                .register(meterRegistry);

        // initialize counters with tags so they appear in metrics
        String[] counterTags = new String[]{"queueName", queueName, "queueId", queueId};
        publishedCounters.computeIfAbsent(queueId, id -> Counter.builder("pulsemq_messages_published").tags(counterTags).description("Published messages").register(meterRegistry));
        consumedCounters.computeIfAbsent(queueId, id -> Counter.builder("pulsemq_messages_consumed").tags(counterTags).description("Consumed messages").register(meterRegistry));
        ackCounters.computeIfAbsent(queueId, id -> Counter.builder("pulsemq_ack_total").tags(counterTags).description("ACK total").register(meterRegistry));
        nackCounters.computeIfAbsent(queueId, id -> Counter.builder("pulsemq_nack_total").tags(counterTags).description("NACK total").register(meterRegistry));
        retryCounters.computeIfAbsent(queueId, id -> Counter.builder("pulsemq_retry_total").tags(counterTags).description("Retry total").register(meterRegistry));
        dlqCounters.computeIfAbsent(queueId, id -> Counter.builder("pulsemq_dlq_total").tags(counterTags).description("DLQ total").register(meterRegistry));

        log.info("Registered metrics for queue {} ({})", queueName, queueId);
    }

    public void incrementPublished(String queueId) {
        Counter c = publishedCounters.get(queueId);
        if (c != null) c.increment();
    }

    public void incrementConsumed(String queueId) {
        Counter c = consumedCounters.get(queueId);
        if (c != null) c.increment();
    }

    public void incrementAck(String queueId) {
        Counter c = ackCounters.get(queueId);
        if (c != null) c.increment();
    }

    public void incrementNack(String queueId) {
        Counter c = nackCounters.get(queueId);
        if (c != null) c.increment();
    }

    public void incrementRetry(String queueId) {
        Counter c = retryCounters.get(queueId);
        if (c != null) c.increment();
    }

    public void incrementDlq(String queueId) {
        Counter c = dlqCounters.get(queueId);
        if (c != null) c.increment();
    }
}


