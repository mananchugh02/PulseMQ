PulseMQ observability
[
This project exposes metrics via Spring Boot Actuator and Micrometer (Prometheus registry).

Endpoints
- /actuator/prometheus - Prometheus metrics endpoint
]()
Prometheus
- Sample config at docs/prometheus.yml

Grafana
- Sample dashboard JSON at docs/grafana-dashboard-pulsemq.json

Metrics exposed (examples)
- pulsemq_queue_depth{queueId,queueName} - gauge of ready messages per queue
- pulsemq_inflight_count{queueId,queueName} - gauge of in-flight messages per queue
- pulsemq_messages_published{queueId,queueName} - counter
- pulsemq_messages_consumed{queueId,queueName} - counter
- pulsemq_ack_total{queueId,queueName} - counter
- pulsemq_nack_total{queueId,queueName} - counter
- pulsemq_retry_total{queueId,queueName} - counter
- pulsemq_dlq_total{queueId,queueName} - counter

WAL persistence
- Append-only JSONL logs are written under `logs/wal`.
- Startup recovery replays archived WAL files before runtime consumers are allowed to process messages.
- Rotated WAL segments are archived under `logs/wal/archive`.

Long polling consume
- `GET /api/queues/{queueId}/consume?timeout=30` blocks until a message is ready or the timeout is reached.
- When a message is available, the broker returns it immediately and marks it in-flight for ACK/NACK handling.
- If no message arrives before the timeout, the broker returns `204 No Content`.

Notes
- Configure Prometheus to scrape /actuator/prometheus on the PulseMQ host/port.
- Tune DLQ growth warning threshold in QueueMetricsService.monitorQueueDepths if needed.

