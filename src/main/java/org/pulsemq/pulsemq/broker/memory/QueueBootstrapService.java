package org.pulsemq.pulsemq.broker.memory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.pulsemq.pulsemq.service.QueueMetricsService;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class QueueBootstrapService {

    private final QueueRepository queueRepository;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;
    private final QueueMetricsService queueMetricsService;

    @EventListener(ApplicationReadyEvent.class)
    public void initializeInMemoryQueues() {
        refreshFromDatabase();
    }

    @Transactional(readOnly = true)
    public void refreshFromDatabase() {
        List<QueueEntity> queueEntities = queueRepository.findAll();
        inMemoryQueueRegistry.replaceAll(queueEntities);
        queueEntities.stream()
                .filter(java.util.Objects::nonNull)
                .forEach(queueMetricsService::registerQueueMetrics);
        log.info("Initialized {} in-memory queue(s) from PostgreSQL", inMemoryQueueRegistry.size());
    }
}

