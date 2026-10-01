# Kafka Architecture Demo (Spring Boot 3 / Java 21)

A runnable reference for Apache Kafka's core architectural patterns:

| Pattern | Where |
|---|---|
| Idempotent producer (`acks=all`, idempotence, infinite retries, 5 in-flight) | `KafkaProducerConfig` |
| Transactional producer / atomic multi-topic writes (EOS) | `KafkaProducerConfig`, `OrderProducerService#publishOrdersAtomically` |
| Custom partitioner (VIP lane + MurmurHash2 for the rest) | `OrderKeyPartitioner` |
| Consumer group, manual commit (`MANUAL_IMMEDIATE`), cooperative rebalancing | `KafkaConsumerConfig`, `OrderConsumerService` |
| Exponential backoff (3 retries, 1s, x2.0) and Dead Letter Topic `orders-dlq` | `KafkaConsumerConfig#orderErrorHandler` |
| Rebalance listener | `CustomRebalanceListener` |
| On-disk log / index / HW visualizer | `KafkaStorageEngineSimulator`, `StorageVisualizerController` |

## Run

```bash
docker compose up -d          # Kafka (KRaft, single node) on :9092, Kafka UI on http://localhost:8081
mvn spring-boot:run           # app on http://localhost:8080 (creates topics on startup)
```

Requires JDK 21, Maven 3.9+, Docker.

Topics: `orders` (6 partitions), `orders-audit` (3), `orders-dlq` (3).

## REST API

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/orders/publish` | Idempotent publish, partition chosen by the custom partitioner |
| POST | `/api/v1/orders/transactional` | Atomic write to `orders` + `orders-audit` |
| GET | `/api/v1/kafka/cluster-status` | Brokers, topics, partitions, offsets, consumer group lag |
| GET | `/api/v1/log-engine/inspect[?lookupOffset=N]` | Segments, index entries, byte positions, LEO vs HW |
| POST | `/api/v1/log-engine/append` | Append one record to the simulated log |
| POST | `/api/v1/log-engine/seed?count=20&replicate=true` | Generate many records (forces segment rolls) |
| POST | `/api/v1/log-engine/replicate?follower=follower-1&count=5` | Simulate a follower fetch |
| POST | `/api/v1/log-engine/followers/{id}/pause` and `/resume` | Stall / resume a follower |
| POST | `/api/v1/log-engine/roll` | Force a new segment |
| GET | `/api/v1/log-engine/read?offset=N&max=10` | Consumer-style read bounded by the HW |
| POST | `/api/v1/log-engine/reset` | Wipe the simulated log |

## Try it

**VIP vs standard routing** (VIP goes to partition 0, others hash across 1-5):

```bash
curl -X POST localhost:8080/api/v1/orders/publish -H 'Content-Type: application/json' \
  -d '{"customerId":"VIP-001","product":"laptop","quantity":1,"amount":1999.99}'

curl -X POST localhost:8080/api/v1/orders/publish -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-42","product":"mouse","quantity":2,"amount":49.90}'
```

**Transaction commit, then abort** (aborted records never reach `read_committed` consumers):

```bash
curl -X POST localhost:8080/api/v1/orders/transactional -H 'Content-Type: application/json' \
  -d '{"orders":[{"customerId":"cust-1","product":"a","quantity":1,"amount":10},
                 {"customerId":"VIP-002","product":"b","quantity":1,"amount":20}]}'

curl -X POST localhost:8080/api/v1/orders/transactional -H 'Content-Type: application/json' \
  -d '{"simulateFailure":true,"orders":[{"customerId":"cust-1","product":"a","quantity":1,"amount":10}]}'
```

**Retries and DLQ** (watch the application log):

| `product` value | Behaviour |
|---|---|
| `FAIL_ONCE` | First delivery fails, retry succeeds |
| `FAIL_ALWAYS` | Retried after 1s, 2s, 4s, then sent to `orders-dlq` |
| `POISON_PILL` (or `amount <= 0`) | Non-retryable, sent to `orders-dlq` immediately |

**Rebalancing:** start a second instance with `mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082`
and watch the `[REBALANCE]` log lines. Because the `CooperativeStickyAssignor` is used, only the moved
partitions are revoked.

**Log engine / HW:**

```bash
curl -X POST 'localhost:8080/api/v1/log-engine/seed?count=30&replicate=true'
curl 'localhost:8080/api/v1/log-engine/inspect?lookupOffset=17'

# Stall a follower: HW stops advancing until it is out of the ISR or catches up
curl -X POST localhost:8080/api/v1/log-engine/followers/follower-2/pause
curl -X POST 'localhost:8080/api/v1/log-engine/seed?count=3&replicate=true'
curl 'localhost:8080/api/v1/log-engine/read?offset=30'
curl -X POST localhost:8080/api/v1/log-engine/followers/follower-2/resume
curl -X POST 'localhost:8080/api/v1/log-engine/replicate?follower=follower-2&count=1000'
```

Files are written under `${java.io.tmpdir}/kafka-log-engine/orders-0/`
(`00000000000000000000.log`, `.index`, `.timeindex`, ...). Every order the consumer processes is also
mirrored into this simulated log (`app.log-engine.mirror-consumed-orders`).

## Design notes and simplifications

- **Two kinds of producers.** A transactional producer factory requires every send to happen inside a
  transaction. The listener thread and the plain publish endpoint are not transactional, so they use
  separate idempotent (non-transactional) factories; the DLQ recoverer does too.
- **Partitioner and other topics.** The custom partitioner applies to both `orders` and `orders-audit`.
  On `orders-audit` (key = orderId) only the tier or configured VIP IDs can reach partition 0.
- **Offsets and transactions.** Commit/abort markers occupy offsets, so end offsets can exceed the number
  of visible records.
- **Simulator simplifications.** Single partition, in-memory index mirrors on-disk files, state is
  reset at startup (no crash recovery), ISR membership uses record lag (`max-follower-lag`) instead of
  Kafka's time-based `replica.lag.time.max.ms`, and each record is stored as a one-record batch.
- **Not included:** security (TLS/SASL), schema registry, multi-broker replication, automated tests.
