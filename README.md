# Real-Time Incident Management Dashboard

A production-style reference implementation for a real-time incident ingestion and dashboard platform.

It contains:

- **Gateway API**: accepts incident signals over HTTP and writes them to Kafka/Redpanda.
- **Processor**: consumes the durable stream, applies severity rules, deduplicates events, updates Redis state, and publishes live updates.
- **Dashboard API**: exposes REST endpoints and a server-sent events stream for live dashboards.
- **Web UI**: a minimal browser dashboard.
- **Load generator**: synthetic incident traffic generator.
- **Unit tests**: validation, severity rules, gateway behavior, processor idempotency, dashboard APIs.

## Architecture

```text
Clients / agents / monitors
        |
        v
+-------------------+       durable append-only log       +-------------------+
| Gateway replicas  |  -->  Kafka / Redpanda topic  -->   | Processor replicas|
+-------------------+                                     +---------+---------+
                                                                    |
                                                      Redis Cluster / AOF state
                                                                    |
                                      +-----------------------------+------------------+
                                      |                                                |
                              Dashboard REST API                              SSE live stream
                                      |                                                |
                                      +-------------------- UI ------------------------+
```

## Semantics

- **Ingestion durability**: gateway returns `202 Accepted` only after Kafka acknowledges the event.
- **Processing guarantee**: processor uses Kafka consumer groups with explicit commits after state update.
- **Duplicate safety**: each event has an `event_id`; Redis tracks processed event IDs and incident dedupe keys.
- **Incident correlation**: `source + external_id` is preferred; otherwise service/region/signal/message are hashed.
- **Live dashboard**: Redis stores current incident state and publishes live incident updates.

## Running locally

```bash
cp .env.example .env
make run
```

Open the dashboard:

```text
http://localhost:8081
```

Submit one incident:

```bash
curl -X POST http://localhost:8080/v1/incidents \
  -H 'content-type: application/json' \
  -d '{
    "source":"prometheus",
    "external_id":"checkout-5xx-001",
    "service":"checkout-api",
    "region":"ap-south-1",
    "signal":"http_5xx_rate",
    "message":"5xx error rate is above threshold",
    "status_code":503,
    "error_rate":0.12,
    "latency_ms":1800
  }'
```

## Tests

```bash
make test
```

## Load testing

Local example:

```bash
go run ./cmd/loadgen --url http://localhost:8080/v1/incidents --rps 10000 --workers 256 --duration 60s
```

For larger local experiments:

```bash
docker compose up --build --scale gateway=4 --scale processor=4
```

## Scaling toward 1M requests/sec

This repository is designed so the target is achieved horizontally. A single laptop or single process will not prove 1M req/sec. For a real 1M req/sec deployment, use:

- L4/L7 load balancer in front of many gateway pods.
- Kafka/Redpanda topic with hundreds of partitions.
- Multiple Kafka brokers with replication factor 3.
- Processor replicas less than or equal to partition count.
- Redis Cluster or sharded state store with AOF enabled.
- Binary payloads such as Protobuf or FlatBuffers for the hottest path if JSON CPU becomes the bottleneck.
- Synthetic and production-like load tests measuring p50/p95/p99 latency, duplicate rate, error rate, and end-to-end lag.

Suggested starting production sizing:

```text
Gateway pods:      50-150 pods, 4-8 vCPU each
Processor pods:    100-300 pods, 4-8 vCPU each
Kafka partitions:  384-1024
Kafka replication: 3
Redis:             cluster mode, sharded by dedupe key
```

The exact numbers depend on payload size, network bandwidth, broker hardware, TLS settings, cloud region, disk latency, and business rules.

## Key endpoints

Gateway:

```text
POST /v1/incidents
GET  /healthz
GET  /readyz
GET  /metrics
```

Dashboard:

```text
GET /v1/incidents?limit=100
GET /v1/metrics
GET /ws/incident-stream   # server-sent events
GET /
```

## Resume-ready project description

**Real-Time Incident Management Dashboard**  
Architected and implemented a horizontally scalable incident ingestion and monitoring platform using Go, Kafka/Redpanda, Redis, server-sent events, and Docker. Designed for fault-tolerant at-least-once processing, idempotent event handling, live incident dashboards, severity classification, and horizontal scaling toward million-request-per-second workloads.


## Production build tags

The default build path keeps tests dependency-light by using in-memory/local adapters. Docker builds use `-tags production`, enabling Kafka and Redis adapters. For manual production runs, use:

```bash
go run -tags production ./cmd/gateway
go run -tags production ./cmd/processor
go run -tags production ./cmd/dashboard
```
