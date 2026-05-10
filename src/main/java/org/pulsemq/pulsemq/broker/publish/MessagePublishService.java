package org.pulsemq.pulsemq.broker.publish;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.DTO.RequestDTO.PublishMessageRequestDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageDeliveryDTO;
import org.pulsemq.pulsemq.DTO.ResponseDTO.PublishMessageResponseDTO;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueue;
import org.pulsemq.pulsemq.broker.memory.InMemoryQueueRegistry;
import org.pulsemq.pulsemq.broker.memory.QueuedMessage;
import org.pulsemq.pulsemq.common.enums.ExchangeType;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.pulsemq.pulsemq.psql.model.BindingEntity;
import org.pulsemq.pulsemq.psql.model.ExchangeEntity;
import org.pulsemq.pulsemq.psql.model.MessageEntity;
import org.pulsemq.pulsemq.psql.model.QueueEntity;
import org.pulsemq.pulsemq.psql.repository.BindingRepository;
import org.pulsemq.pulsemq.psql.repository.ExchangeRepository;
import org.pulsemq.pulsemq.psql.repository.MessageRepository;
import org.pulsemq.pulsemq.psql.repository.QueueRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessagePublishService {

    private final ExchangeRepository exchangeRepository;
    private final BindingRepository bindingRepository;
    private final QueueRepository queueRepository;
    private final MessageRepository messageRepository;
    private final InMemoryQueueRegistry inMemoryQueueRegistry;

    @Transactional
    public PublishMessageResponseDTO publishMessage(PublishMessageRequestDTO requestDTO) {
        validateRequest(requestDTO);

        ExchangeEntity exchange = exchangeRepository.getExchangeById(requestDTO.getExchangeId())
                .orElseThrow(() -> new EntityNotFoundException("Exchange not found for id: " + requestDTO.getExchangeId()));

        List<BindingEntity> allBindings = bindingRepository.findAllByExchange_Id(exchange.getId());
        List<BindingEntity> matchedBindings = resolveMatchedBindings(exchange.getType(), allBindings, requestDTO.getRoutingKey());

        if (matchedBindings.isEmpty()) {
            throw new IllegalArgumentException("No bindings matched exchange " + exchange.getId() + " for routing key: " + requestDTO.getRoutingKey());
        }

        Map<UUID, QueueEntity> targetQueues = resolveTargetQueues(matchedBindings);
        Instant now = Instant.now();

        List<MessageEntity> persistedMessages = new ArrayList<>(targetQueues.size());
        for (QueueEntity queueEntity : targetQueues.values()) {
            persistedMessages.add(MessageEntity.builder()
                    .queue(queueEntity)
                    .payload(requestDTO.getPayload())
                    .status(MessageStatus.READY)
                    .retryCount(0)
                    .visibleAt(now)
                    .build());
        }

        List<MessageEntity> savedMessages = messageRepository.saveAll(persistedMessages);
        List<PublishMessageDeliveryDTO> deliveries = new ArrayList<>(savedMessages.size());

        for (MessageEntity savedMessage : savedMessages) {
            QueueEntity queueEntity = Objects.requireNonNull(savedMessage.getQueue(), "savedMessage queue must not be null");
            InMemoryQueue inMemoryQueue = ensureInMemoryQueue(queueEntity);
            QueuedMessage queuedMessage = QueuedMessage.builder()
                    .messageId(savedMessage.getId())
                    .queueId(queueEntity.getId())
                    .routingKey(requestDTO.getRoutingKey())
                    .payload(savedMessage.getPayload())
                    .headers(requestDTO.getHeaders())
                    .enqueuedAt(now)
                    .build();

            boolean accepted = inMemoryQueue.enqueue(queuedMessage);
            if (!accepted) {
                throw new IllegalStateException("Unable to enqueue message into in-memory queue: " + queueEntity.getId());
            }

            deliveries.add(PublishMessageDeliveryDTO.builder()
                    .queueId(queueEntity.getId())
                    .queueName(queueEntity.getName())
                    .messageId(savedMessage.getId())
                    .status(savedMessage.getStatus())
                    .visibleAt(savedMessage.getVisibleAt())
                    .build());
        }

        log.info("Published message to {} queue(s) for exchange {}", deliveries.size(), exchange.getId());
        return PublishMessageResponseDTO.builder()
                .exchangeId(exchange.getId())
                .exchangeName(exchange.getName())
                .exchangeType(exchange.getType().name())
                .routingKey(requestDTO.getRoutingKey())
                .matchedQueues(deliveries.size())
                .savedMessages(savedMessages.size())
                .publishedAt(now)
                .deliveries(deliveries.stream()
                        .sorted(Comparator.comparing(PublishMessageDeliveryDTO::getQueueName, Comparator.nullsLast(String::compareToIgnoreCase)))
                        .toList())
                .build();
    }

    private void validateRequest(PublishMessageRequestDTO requestDTO) {
        if (requestDTO == null) {
            throw new IllegalArgumentException("Publish request must not be null");
        }
        if (requestDTO.getExchangeId() == null) {
            throw new IllegalArgumentException("Exchange id must not be null");
        }
        if (!StringUtils.hasText(requestDTO.getPayload())) {
            throw new IllegalArgumentException("Payload must not be blank");
        }
    }

    private List<BindingEntity> resolveMatchedBindings(ExchangeType exchangeType, List<BindingEntity> allBindings, String routingKey) {
        if (exchangeType == null) {
            throw new IllegalArgumentException("Exchange type must not be null");
        }

        if (exchangeType != ExchangeType.FANOUT && !StringUtils.hasText(routingKey)) {
            throw new IllegalArgumentException("Routing key must not be blank for " + exchangeType + " exchange");
        }

        if (allBindings == null || allBindings.isEmpty()) {
            return List.of();
        }

        return switch (exchangeType) {
            case FANOUT -> allBindings;
            case DIRECT -> allBindings.stream()
                    .filter(binding -> routingKey.equals(binding.getRoutingKey()))
                    .toList();
            case TOPIC -> allBindings.stream()
                    .filter(binding -> matchesTopic(binding.getRoutingKey(), routingKey))
                    .toList();
        };
    }

    private Map<UUID, QueueEntity> resolveTargetQueues(List<BindingEntity> matchedBindings) {
        Map<UUID, QueueEntity> targetQueues = new LinkedHashMap<>();

        for (BindingEntity binding : matchedBindings) {
            if (binding == null || binding.getQueue() == null || binding.getQueue().getId() == null) {
                continue;
            }

            UUID queueId = binding.getQueue().getId();
            if (targetQueues.containsKey(queueId)) {
                continue;
            }

            QueueEntity queueEntity = binding.getQueue();
            if (queueEntity.getName() == null || queueEntity.getType() == null) {
                queueEntity = queueRepository.getQueueById(queueId)
                        .orElseThrow(() -> new EntityNotFoundException("Queue not found for id: " + queueId));
            }

            targetQueues.put(queueId, queueEntity);
        }

        return targetQueues;
    }

    private InMemoryQueue ensureInMemoryQueue(QueueEntity queueEntity) {
        return inMemoryQueueRegistry.getQueue(queueEntity.getId())
                .orElseGet(() -> inMemoryQueueRegistry.registerQueue(queueEntity));
    }

    private boolean matchesTopic(String bindingKey, String routingKey) {
        if (!StringUtils.hasText(bindingKey) || !StringUtils.hasText(routingKey)) {
            return false;
        }

        String[] patternParts = bindingKey.split("\\.");
        String[] routingParts = routingKey.split("\\.");
        return matchesTopic(patternParts, 0, routingParts, 0);
    }

    private boolean matchesTopic(String[] patternParts, int patternIndex, String[] routingParts, int routingIndex) {
        if (patternIndex == patternParts.length) {
            return routingIndex == routingParts.length;
        }

        String token = patternParts[patternIndex];
        if ("#".equals(token)) {
            if (patternIndex == patternParts.length - 1) {
                return true;
            }
            for (int i = routingIndex; i <= routingParts.length; i++) {
                if (matchesTopic(patternParts, patternIndex + 1, routingParts, i)) {
                    return true;
                }
            }
            return false;
        }

        if (routingIndex >= routingParts.length) {
            return false;
        }

        if ("*".equals(token) || token.equals(routingParts[routingIndex])) {
            return matchesTopic(patternParts, patternIndex + 1, routingParts, routingIndex + 1);
        }

        return false;
    }
}

