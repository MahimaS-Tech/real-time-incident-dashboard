# Production sizing notes for 1M req/sec

This implementation uses a horizontally scalable architecture. A 1M req/sec claim should be validated with a benchmark in the target environment.

## Hot path

```text
HTTP gateway -> Kafka/Redpanda partition -> processor -> Redis state/pubsub -> dashboard
```

## Capacity knobs

1. **Gateway replicas**: scale by CPU, network, and Kafka write latency.
2. **Kafka partitions**: upper bound for parallel consumer-group processing.
3. **Processor replicas**: should not exceed useful partition count by much.
4. **Redis state**: use Redis Cluster or shard by dedupe key for large cardinality.
5. **Payload format**: JSON is easiest to inspect; Protobuf can reduce CPU and bandwidth.
6. **Batching**: tune Kafka batch size and timeout based on p99 latency budget.
7. **Idempotency TTL**: longer TTL improves duplicate protection but consumes more memory.

## Suggested validation metrics

- Gateway accepted requests/sec.
- Gateway p50/p95/p99 latency.
- Kafka produce latency.
- Kafka consumer lag.
- Processor events/sec.
- Redis command latency.
- End-to-end event-to-dashboard latency.
- Duplicate event count.
- Error rate under broker/processor/Redis restarts.

## Failure tests

- Kill a gateway pod during load: clients should retry; already acknowledged events remain in Kafka.
- Kill a processor pod during load: Kafka reassigns partitions; uncommitted messages replay.
- Restart Redis with AOF enabled: state should recover to last fsync window.
- Add duplicate event IDs: incident count should not increase.
- Send multiple event IDs with same external ID: incident count should increase under one correlated incident.
