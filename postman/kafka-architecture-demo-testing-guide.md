# Kafka Architecture Demo - Testing Guide


End-to-end test suite for the Spring Boot / Kafka application. Every request has a description with
**purpose, steps and expected result**, plus automated tests (Tests tab).

## Setup
1. Import this file into Postman. Variables live on the collection (Variables tab): `baseUrl` = http://localhost:8080,
   `baseUrl2` = http://localhost:8082 (only for the rebalance test).
2. Start infrastructure: `docker compose up -d` (Kafka KRaft + Kafka UI at http://localhost:8081).
3. Start the app: `mvn spring-boot:run`.
4. Open the Postman Console (View -> Show Postman Console) to see logs and the partition histogram.

## Run order
Run the folders **in order** with *Run collection* (Runner), iterations = 1, except:
- **1.7** is meant to be run separately with 30 iterations (skip it in the full run by unchecking it, or keep it with 1 iteration).
- **Folder 04** needs a second app instance started manually (see its description); uncheck it for an unattended run.
- **Folder 05** must run in sequence and without other order traffic (the consumer mirrors orders into the simulator).

| Folder | Verifies |
|---|---|
| 00 | Environment healthy; baselines captured |
| 01 | Idempotent producer, VIP partitioner, key affinity, validation |
| 02 | Transactional commit and abort (multi-topic atomicity) |
| 03 | Manual ack, exponential backoff retry, DLQ, counters |
| 04 | Rebalance listener, cooperative-sticky redistribution |
| 05 | Log segments, sparse index, LEO vs HW, ISR behaviour |

## Useful external checks
- Kafka UI: http://localhost:8081 -> Topics -> `orders`, `orders-audit`, `orders-dlq`; Consumers -> `order-processing-group`.
- `docker exec -it kafka-kraft /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-processing-group`
- Application log keywords: `[CONSUME]`, `[RETRY]`, `[DLQ]`, `[DLQ-MONITOR]`, `[REBALANCE]`.

## Troubleshooting
- 503 from cluster-status / publish: broker not reachable - check `docker ps` and `spring.kafka.bootstrap-servers`.
- Exact-count assertion in 3.5 fails: other traffic or an app restart occurred; re-run from 0.2.
- Log-engine numbers off: an order was consumed (mirrored) after the reset; run 5.1 again and avoid publishing.

---


## 00 - Prerequisites & Baseline

**Goal:** prove the environment is healthy and capture baseline counters used by later assertions.

**Before you start**
1. `docker compose up -d` and wait until `docker ps` shows `kafka-kraft` as *healthy*.
2. `mvn spring-boot:run` (app on port 8080). Wait for the log line `Started KafkaArchitectureApplication`
   and the three `[REBALANCE] ... ASSIGNED` lines (one per listener thread).
3. Kafka UI is on http://localhost:8081 (cluster `local-kraft`) - keep it open for visual checks.
4. Run this folder FIRST: request 2 resets the `expectedProcessed` / `expectedDead` counters.


### 0.1 Actuator health

`GET {{baseUrl}}/actuator/health`


**Purpose:** confirm the Spring Boot app is up.

**Steps:** send the request.

**Expected:** HTTP 200, `{"status":"UP"}`.


### 0.2 Cluster status (BASELINE)

`GET {{baseUrl}}/api/v1/kafka/cluster-status`


**Purpose:** verify topics/partitions exist, the consumer group formed, and store baseline counters.

**Steps:** send the request.

**Expected**
- `cluster.brokers` has 1 broker (KRaft node id 1), `cluster.clusterId` = `MkU3OEVBNTcwNTJENDM2Qk`.
- Topics: `orders` = 6 partitions, `orders-audit` = 3, `orders-dlq` = 3; each partition has leader 1 and isr [1].
- Group `order-processing-group`: state `Stable`, 3 members (concurrency=3), assignor `cooperative-sticky`.

**Side effect:** saves `baselineProcessed`, `baselineDead` and zeroes `expectedProcessed`, `expectedDead`.

**If it returns 503:** Kafka is not reachable or topics were not created yet - wait 10 s and retry.


## 01 - Producer & Custom Partitioner

**Feature under test:** idempotent producer (`acks=all`, idempotence, retries=MAX, in-flight=5) and
`OrderKeyPartitioner` (VIP -> partition 0, standard -> MurmurHash2 over partitions 1-5).

**Key facts**
- Record key = `customerId`, so one customer always maps to one partition (per-key ordering).
- VIP = `customerId` in `VIP-001,VIP-002,VIP-003` OR `customerTier = VIP`.
- Each successful publish is eventually consumed; tests add to `expectedProcessed`.

**Visual check (Kafka UI):** Topics -> `orders` -> Messages. Filter by partition 0: only VIP orders appear.
Partitions 1-5 hold standard orders.

**CLI check:**
`docker exec -it kafka-kraft /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --property print.partition=true --property print.key=true`


### 1.1 Publish VIP order (by configured customer ID)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "VIP-001",
  "product": "laptop",
  "quantity": 1,
  "amount": 1999.99
}
```


**Purpose:** VIP customer ID is routed to the dedicated partition.

**Steps:** send the request (customer `VIP-001`).

**Expected:** 200, `receipt.partition` = 0, `routedToDedicatedVipPartition` = true, `receipt.topic` = `orders`.

**Also check:** app log shows `[CONSUME] orders-0@<offset> key=VIP-001`.


### 1.2 Publish VIP order (by tier, unknown customer ID)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-7",
  "product": "monitor",
  "quantity": 1,
  "amount": 350,
  "customerTier": "VIP"
}
```


**Purpose:** the partitioner also inspects the event value - `customerTier=VIP` routes to partition 0
even when the ID is not in the VIP list.

**Expected:** 200, `receipt.partition` = 0.


### 1.3 Publish STANDARD order (cust-42)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-42",
  "product": "mouse",
  "quantity": 2,
  "amount": 49.9
}
```


**Purpose:** standard customers must NEVER land on partition 0.

**Expected:** 200, `receipt.partition` between 1 and 5, `routedToDedicatedVipPartition` = false.
The partition is saved in `custA_partition` for the next request.


### 1.4 Publish STANDARD order again (same key -> same partition)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-42",
  "product": "keyboard",
  "quantity": 1,
  "amount": 89
}
```


**Purpose:** key affinity - MurmurHash2(key) is deterministic, so ordering per customer is preserved.

**Expected:** partition equals the one recorded in request 1.3, and the offset is higher.


### 1.5 Validation error - missing customerId

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "product": "x",
  "quantity": 1,
  "amount": 5
}
```


**Purpose:** bean validation rejects bad input before anything reaches Kafka.

**Expected:** HTTP 400, nothing published.


### 1.6 Validation error - quantity 0

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-1",
  "product": "x",
  "quantity": 0,
  "amount": 5
}
```


**Purpose:** `@Min(1)` on quantity. **Expected:** HTTP 400.


### 1.7 Partition distribution (RUN 30x in Collection Runner)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-{{$randomInt}}-{{$randomAlphaNumeric}}",
  "product": "widget",
  "quantity": 1,
  "amount": 12.5
}
```


**Purpose:** show MurmurHash2 spreading many standard keys across partitions 1-5 and none on 0.

**Steps**
1. Right-click this request -> *Run* (or Runner with only this request), **Iterations = 30**, delay 0.
2. Open the Postman Console (View -> Show Postman Console): the histogram is logged every iteration.

**Expected:** every iteration passes (partition 1-5); final histogram has 5 keys (1..5), none for 0.
In Kafka UI, partitions 1-5 of `orders` fill roughly evenly.


## 02 - Transactional Producer (Exactly-Once writes)

**Feature under test:** `@Transactional` + `KafkaTransactionManager` writing to `orders` and `orders-audit`
atomically.

**Consumers use `isolation.level=read_committed`**, so aborted records are never delivered.

**CLI check for visibility (run after 2.2):**
`docker exec -it kafka-kraft /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders-audit --from-beginning --isolation-level read_committed --property print.key=true`
-> shows audit records of 2.1 but NOT of 2.2. With `--isolation-level read_uncommitted` the aborted ones appear too.


### 2.1 Commit - 2 orders -> 4 records in 2 topics

`POST {{baseUrl}}/api/v1/orders/transactional`


Body:
```json
{
  "simulateFailure": false,
  "orders": [
    {
      "customerId": "cust-tx-1",
      "product": "tx-product-A",
      "quantity": 1,
      "amount": 10
    },
    {
      "customerId": "VIP-002",
      "product": "tx-product-B",
      "quantity": 1,
      "amount": 20
    }
  ]
}
```


**Purpose:** one transaction, two topics, several partitions.

**Steps:** send. The batch contains a standard order (`cust-tx-1`) and a VIP order (`VIP-002`).

**Expected:** 200, `status` = COMMITTED, `recordCount` = 4 (2 x `orders` + 2 x `orders-audit`).
The `VIP-002` record in `orders` is on partition 0. App log: `Transaction of 4 records will COMMIT`.


### 2.2 Abort - simulateFailure=true

`POST {{baseUrl}}/api/v1/orders/transactional`


Body:
```json
{
  "simulateFailure": true,
  "orders": [
    {
      "customerId": "cust-tx-abort",
      "product": "SHOULD_NEVER_BE_CONSUMED",
      "quantity": 1,
      "amount": 99
    }
  ]
}
```


**Purpose:** records are written to the broker, then the transaction is aborted.

**Steps:** send (product `SHOULD_NEVER_BE_CONSUMED`).

**Expected:** HTTP 409, `status` = ABORTED, reason mentions the simulated failure. App log:
`Simulating failure ... transaction will ABORT`.

**Proof of atomicity:** the listener never logs `[CONSUME]` for this product, and request 3.5
(exact counter equality) would fail if an aborted record had been processed.


### 2.3 Validation error - empty batch

`POST {{baseUrl}}/api/v1/orders/transactional`


Body:
```json
{
  "simulateFailure": false,
  "orders": []
}
```


**Expected:** HTTP 400 (`@NotEmpty orders`).


## 03 - Consumer, Retry & Dead Letter Queue

**Feature under test:** manual ack (`MANUAL_IMMEDIATE`), `DefaultErrorHandler` with exponential backoff
(3 retries: 1 s, 2 s, 4 s) and routing to `orders-dlq`.

| product | behaviour | counter effect |
|---|---|---|
| `FAIL_ONCE` | fails first delivery, retry succeeds | processed +1 |
| `FAIL_ALWAYS` | 4 attempts (t=0,1,3,7 s) then DLQ | dead +1 |
| `POISON_PILL` | non-retryable, DLQ at once | dead +1 |
| `amount <= 0` | non-retryable, DLQ at once | dead +1 |

**Watch the app log** while running: `[RETRY] attempt N failed`, `[DLQ] Routing record ...`,
`[RECOVERED] ...`, `[DLQ-MONITOR] ... exception=... message=...`.
In Kafka UI open topic `orders-dlq` -> Messages -> Headers: `kafka_dlt-original-topic`, `-partition`,
`-offset`, `kafka_dlt-exception-message`.


### 3.1 FAIL_ONCE (transient failure, succeeds on retry)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-retry-1",
  "product": "FAIL_ONCE",
  "quantity": 1,
  "amount": 25
}
```


**Steps:** send, then watch the log: attempt 1 fails ("Transient failure"), ~1 s later the retry succeeds.

**Expected:** publish returns 200 immediately (async consumer). No DLQ record.


### 3.2 FAIL_ALWAYS (retries exhausted -> DLQ)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-retry-2",
  "product": "FAIL_ALWAYS",
  "quantity": 1,
  "amount": 25
}
```


**Steps:** send, watch the log for ~7 s: 4 failed attempts with growing delays, then
`[DLQ] Routing record orders-<p>@<o> to 'orders-dlq'`.

**Expected:** publish 200; one record appears in `orders-dlq` within ~10 s.


### 3.3 POISON_PILL (non-retryable -> immediate DLQ)

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-poison",
  "product": "POISON_PILL",
  "quantity": 1,
  "amount": 25
}
```


**Steps:** send; the log shows NO retry attempts - the record is dead-lettered on the first failure.

**Expected:** 200; DLQ record with `kafka_dlt-exception-fqcn` ending in `NonRetryableOrderException`.


### 3.4 Invalid amount (-5) -> DLQ

`POST {{baseUrl}}/api/v1/orders/publish`


Body:
```json
{
  "customerId": "cust-badamount",
  "product": "gadget",
  "quantity": 1,
  "amount": -5
}
```


**Purpose:** the API accepts any non-null amount; the consumer's business rule rejects `amount <= 0`
as non-retryable.

**Expected:** 200 from the API; DLQ record shortly after.


### 3.5 VERIFY counters (waits, then exact-match assertions)

`GET {{baseUrl}}/api/v1/kafka/cluster-status`


**Purpose:** prove every published order was either processed or dead-lettered - and that the
aborted transaction (2.2) was never processed.

**Steps:** run after folders 01-03. The pre-request script waits `settleDelayMs` (default 15 s) so
FAIL_ALWAYS (7 s of backoff) completes. If the wait is unsupported in your Postman version,
wait ~15 s manually.

**Expected:** `processedOrders - baselineProcessed` = `expectedProcessed` and
`deadLetteredOrders - baselineDead` = `expectedDead`. Any other traffic or a restart of the app
during the run invalidates exact equality - re-run from request 0.2.


### 3.6 Cluster status after processing (group health & lag)

`GET {{baseUrl}}/api/v1/kafka/cluster-status`


**Purpose:** inspect committed offsets (manual acks) and lag.

**Expected:** `order-processing-group` Stable with 3 members, 6 partitions assigned in total
(2 per member); `orders-dlq-monitor-group` has 1 member.

**About lag:** transaction commit/abort markers occupy offsets but are not records, so a small
residual lag (a few offsets) can remain on partitions whose last entry is a marker. That is normal.


## 04 - Consumer Group Rebalance (manual + observation)

**Feature under test:** `CustomRebalanceListener` and cooperative-sticky rebalancing.

**Steps**
1. Run 4.1 (single instance: 3 members, 2 partitions each).
2. In a second terminal start instance 2:
   `mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082`
3. Watch BOTH consoles: instance 1 logs `[REBALANCE] ... REVOKING ...` for some partitions, instance 2 logs
   `[REBALANCE] ... ASSIGNED ...`. With the cooperative assignor only the moved partitions are revoked.
4. Wait ~10 s, then run 4.2 and 4.3.
5. Stop instance 2 (Ctrl+C) and re-run 4.1: members return to 3 and instance 1 logs ASSIGNED for the
   partitions it gets back.


### 4.1 Cluster status - instance 1 (single instance)

`GET {{baseUrl}}/api/v1/kafka/cluster-status`


**Expected (only instance 1 running):** 3 members, each with 2 partitions.


### 4.2 Cluster status after starting instance 2

`GET {{baseUrl}}/api/v1/kafka/cluster-status`


**Expected:** 6 members (3 threads x 2 instances), each owning exactly 1 of the 6 `orders` partitions.
Uses the instance-1 URL; the group view is cluster-wide.


### 4.3 Rebalance listener snapshot - instance 1

`GET {{baseUrl}}/api/v1/kafka/cluster-status`


**Purpose:** `application.rebalanceListener` exposes counts and the partitions each listener thread owns.

**Expected:** `revokedEvents >= 1` (instance 1 gave partitions to instance 2), `assignedEvents >= 3`
(initial assignment), and each thread's list is shorter than before.


### 4.4 Cluster status - instance 2 (port 8082)

`GET {{baseUrl2}}/api/v1/kafka/cluster-status`


**Purpose:** instance 2 reports the same group and its own listener-thread assignments.

**Expected:** 200 and `rebalanceListener.assignedEvents >= 1`.


## 05 - Log Engine Simulator (segments, index, LEO vs HW)

**Feature under test:** `KafkaStorageEngineSimulator` - `.log` / `.index` / `.timeindex` files, sparse index
lookups, segment rolling, LEO, High Watermark, ISR.

**Config in effect:** `segment-bytes=1024`, `index-interval-bytes=256`, `max-follower-lag=5`, followers
`follower-1`, `follower-2`.

**IMPORTANT:** the consumer mirrors every processed order into this same simulated log
(`app.log-engine.mirror-consumed-orders=true`). Run this folder in order, starting with 5.1 (reset),
and do NOT publish orders meanwhile - otherwise the exact LEO/HW numbers differ. (Or start the app with
`--app.log-engine.mirror-consumed-orders=false`.)

**Disk check:** open `<java.io.tmpdir>/kafka-log-engine/orders-0/` (e.g. `/tmp/kafka-log-engine/orders-0`)
and run `ls -l` / `xxd 00000000000000000000.index | head` while following along.

**Model recap:** LEO = next offset the leader assigns. HW = min(LEO of in-sync replicas). Consumers read
only offsets < HW. A follower more than 5 records behind leaves the ISR and stops holding back the HW.


### 5.1 Reset simulated log

`POST {{baseUrl}}/api/v1/log-engine/reset`


**Expected:** `{"status":"RESET"}`. Old segment files are deleted; new segment `000...0.log` is created.


### 5.2 Inspect empty log

`GET {{baseUrl}}/api/v1/log-engine/inspect`


**Expected:** LEO = 0, HW = 0, exactly one (active) segment with base offset 0, no records, no index
entries; 3 replicas (leader, follower-1, follower-2) all in sync.


### 5.3 Append one record (replicated)

`POST {{baseUrl}}/api/v1/log-engine/append`


Body:
```json
{
  "key": "customer-A",
  "value": "{\"orderId\":\"MANUAL-1\",\"amount\":10}",
  "replicate": true
}
```


**Expected:** offset 0 at byte position 0; `segmentRolled` false; LEO 1; HW 1 (followers fetched it).


### 5.4 Reset again (clean slate for the numbers below)

`POST {{baseUrl}}/api/v1/log-engine/reset`


**Purpose:** 5.3 left one record in the log. Reset so the following steps use round numbers
(offsets 0..29 after the next request).

**Expected:** `{"status":"RESET"}`.


### 5.5 Seed 30 records (forces segment rolls)

`POST {{baseUrl}}/api/v1/log-engine/seed?count=30&replicate=true`


**Purpose:** generate enough data to roll segments (`segment-bytes=1024`, ~100 bytes per record).

**Expected:** 30 records appended; at least 2 segment rolls; last offset 29, LEO 30, HW 30
(both followers fetch every record because `replicate=true`).


### 5.6 Inspect: segments, index, byte positions

`GET {{baseUrl}}/api/v1/log-engine/inspect`


**What to read in the response**
- `segments[]` - one entry per `.log/.index/.timeindex` triple; names are the zero-padded base offset.
- `segments[i].indexEntries` - sparse index: `relativeOffset -> filePosition`, roughly every 256 bytes.
- `segments[i].records` - each record's `offset`, `position` (byte), `sizeBytes`, `crcValid`.
- `activeSegment` - the segment still being written (last one).

**Expected:** segments are contiguous (next base offset = previous base + record count), records are
laid out back-to-back (`position[n+1] = position[n] + sizeBytes[n]`), CRCs valid, record counts sum to 30.

Note: `timeIndexEntries` may be empty for rapid appends (an entry needs a strictly newer timestamp).


### 5.7 Offset lookup trace (offset 17)

`GET {{baseUrl}}/api/v1/log-engine/inspect?lookupOffset=17`


**Purpose:** show how Kafka finds an offset: pick the segment by base offset -> binary search the sparse
index -> scan the `.log` forward from that byte position.

**Steps:** read `lookupTrace.explanation` - it lists the segment, index entry used, binary-search
steps, and how many records/bytes were scanned.

**Expected:** `found` true; the scan touches only a few records, not the whole segment.


### 5.8 Lookup out of range (offset 9999)

`GET {{baseUrl}}/api/v1/log-engine/inspect?lookupOffset=9999`


**Expected:** `lookupTrace.found` false with an out-of-range explanation (valid range [0, LEO)).


### 5.9 Consumer-style read (offsets 5..7)

`GET {{baseUrl}}/api/v1/log-engine/read?offset=5&max=3`


**Expected:** exactly 3 records with offsets 5, 6, 7, valid CRCs, and a `lookupTrace`.


### 5.10 Pause follower-2

`POST {{baseUrl}}/api/v1/log-engine/followers/follower-2/pause`


**Purpose:** simulate a slow/stalled follower. It is still in the ISR until it lags by more than 5.

**Expected:** `pausedFollowers` contains follower-2; ISR still has all 3 replicas.


### 5.11 Seed 3 while follower-2 is paused -> HW stays at 30

`POST {{baseUrl}}/api/v1/log-engine/seed?count=3&replicate=true`


**Purpose:** show LEO racing ahead of HW.

**Expected:** LEO 33, HW 30 - follower-2 is 3 behind (<= 5, still ISR) and holds the HW back.


### 5.12 Read offset 30 -> hidden (above HW)

`GET {{baseUrl}}/api/v1/log-engine/read?offset=30&max=5`


**Purpose:** offsets 30-32 exist on the leader but are NOT committed, so consumers cannot see them.

**Expected:** empty `records`; `note` explains the offset is at/above the high watermark.


### 5.13 Replicate a paused follower -> 409

`POST {{baseUrl}}/api/v1/log-engine/replicate?follower=follower-2&count=1`


**Expected:** HTTP 409 with an error saying follower-2 is paused.


### 5.14 Replicate unknown follower -> 400

`POST {{baseUrl}}/api/v1/log-engine/replicate?follower=follower-9&count=1`


**Expected:** HTTP 400, `Unknown follower 'follower-9'`.


### 5.15 Seed 5 more -> follower-2 leaves ISR, HW jumps to 38

`POST {{baseUrl}}/api/v1/log-engine/seed?count=5&replicate=true`


**Purpose:** ISR shrink. Once follower-2 is more than 5 records behind (lag 8 at the end) it is
dropped from the ISR, so the HW is computed from leader + follower-1 only.

**Expected:** LEO 38 and HW 38.


### 5.16 Inspect replicas after ISR shrink

`GET {{baseUrl}}/api/v1/log-engine/inspect`


**Expected:** ISR = [leader, follower-1]; follower-2 `inSync` false, `paused` true, lag 8;
`messagesAboveHighWatermark` 0.


### 5.17 Resume follower-2

`POST {{baseUrl}}/api/v1/log-engine/followers/follower-2/resume`


**Expected:** `pausedFollowers` no longer lists follower-2 (it is still out of the ISR until it catches up).


### 5.18 Follower-2 catches up (fetch 1000)

`POST {{baseUrl}}/api/v1/log-engine/replicate?follower=follower-2&count=1000`


**Expected:** follower-2 LEO = 38 (capped at the leader's LEO) and it re-joins the ISR (3 replicas).


### 5.19 Append WITHOUT replication (LEO 39, HW 38)

`POST {{baseUrl}}/api/v1/log-engine/append`


Body:
```json
{
  "key": "manual-1",
  "value": "{\"orderId\":\"MANUAL-2\",\"amount\":77}",
  "replicate": false
}
```


**Purpose:** a record the followers have not fetched yet is not committed.

**Expected:** offset 38, LEO 39, HW still 38.


### 5.20 follower-1 fetches -> HW unchanged

`POST {{baseUrl}}/api/v1/log-engine/replicate?follower=follower-1&count=1`


**Purpose:** the HW is the MIN over the ISR: follower-2 (LEO 38, still in ISR) holds it at 38.

**Expected:** follower-1 LEO 39, HW 38.


### 5.21 follower-2 fetches -> HW advances to 39

`POST {{baseUrl}}/api/v1/log-engine/replicate?follower=follower-2&count=1`


**Expected:** all ISR members have offset 38 -> HW = 39; the record becomes visible to consumers.


### 5.22 Read offset 38 -> now visible

`GET {{baseUrl}}/api/v1/log-engine/read?offset=38&max=5`


**Expected:** one record: offset 38, key `manual-1`.


### 5.23 Read at LEO (offset 39) -> out of range

`GET {{baseUrl}}/api/v1/log-engine/read?offset=39&max=5`


**Expected:** no records; the lookup trace reports out-of-range.


### 5.24 Force segment roll

`POST {{baseUrl}}/api/v1/log-engine/roll`


**Expected:** `newActiveSegmentBaseOffset` = 39 (the LEO). On disk a new
`00000000000000000039.log/.index/.timeindex` triple appears.


### 5.25 Inspect after roll

`GET {{baseUrl}}/api/v1/log-engine/inspect`


**Expected:** active segment is `00000000000000000039.log`, empty; previous segment sealed.


### 5.26 Validation error - blank value

`POST {{baseUrl}}/api/v1/log-engine/append`


Body:
```json
{
  "key": "k",
  "value": "",
  "replicate": true
}
```


**Expected:** HTTP 400 (`@NotBlank value`).
