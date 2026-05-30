# Module 3 — Advanced Topic Design & Data Modeling

Elephant Scale

---

## Module 3 Agenda

- Partition design and key selection
- Avoiding data skew and hot partitions
- Topic compaction vs deletion
- Event schema design
- Schema evolution and compatibility
- Tiered storage and topic lifecycle management
- Cross-cluster replication
- Multi-region streaming architectures

---

## Why Topic Design Matters

Poor topic design leads to:
- **Hot partitions** — one partition handles all the load
- **Schema drift** — consumers break after schema changes
- **Unbounded storage** — topics grow without limits
- **Replication lag** — cross-region pipelines fall behind
- **Operational complexity** — hundreds of poorly named topics

Good design is **the foundation** of a reliable streaming platform.

---

## Partition Count — How to Choose

Partition count determines **maximum parallelism**.

Rules of thumb:
- More partitions = more parallelism = more throughput
- More partitions = more file handles, more rebalance time, more memory
- Start with `max(target throughput / per-partition throughput, consumer count)`

Typical guidance:

 Throughput  Recommended Partitions
----------
 < 100 MB/s  1–6
 100 MB/s – 1 GB/s  6–30
 > 1 GB/s  30–100+

> You can increase partitions later, but **key-based ordering breaks** — plan ahead.

---

## Partition Key Selection

The partition key determines **which partition an event goes to**.

```
partition = hash(key) % numPartitions
```

Good keys:
- `customer_id` — distributes customer events, preserves per-customer order
- `device_id` — distributes IoT telemetry
- `order_id` — distributes order events

Bad keys:
- `null` — round-robin, loses ordering
- `"US"` — if 80% of events are from US, 80% go to one partition
- `timestamp` — sequential keys cause monotonic partition assignment

---

## Data Skew and Hot Partitions

**Hot partition:** one partition receives disproportionately more traffic.

```
Before fix (key = country):
  Partition 0 (US):  ████████████████████ 80%
  Partition 1 (EU):  ████ 15%
  Partition 2 (APAC): █ 5%

After fix (key = user_id):
  Partition 0:  ███████ 33%
  Partition 1:  ███████ 34%
  Partition 2:  ██████  33%
```

Fixes:
- Add entropy to the key: `country + "_" + user_id`
- Use a custom partitioner
- Increase partitions for hot keys (sticky partitioning pattern)

---

## Topic Retention: Deletion vs Compaction

**Delete retention** — events are deleted after time or size threshold:

```
cleanup.policy=delete
retention.ms=604800000   (7 days)
retention.bytes=10737418240  (10 GB per partition)
```

**Compacted retention** — keep only the latest event per key:

```
cleanup.policy=compact
min.cleanable.dirty.ratio=0.5
```

**Combined** — compact AND delete (Kafka 2.0+):

```
cleanup.policy=compact,delete
```

Use compaction for **state** (current value matters), deletion for **events** (history matters).

---

## Event Schema Design Principles

Design events for **longevity and evolvability**:

1. Use **explicit schemas** — never rely on untyped JSON in production
2. Include **metadata fields** in every event:
   - `event_id` (UUID)
   - `event_type`
   - `event_timestamp`
   - `schema_version`
   - `source_service`
3. Use **past-tense naming** — `OrderPlaced`, not `PlaceOrder`
4. Design for **consumer needs** — include all fields consumers need to avoid joins
5. **Never delete fields** — only add optional fields

---

## Schema Formats Compared

 Format  Schema Language  Binary  Human-readable  IDL
--------
 Avro  JSON schema  Yes  No  Schema in registry
 Protobuf  `.proto` files  Yes  No  Strong typing
 JSON Schema  JSON  No  Yes  JSON
 Thrift  Thrift IDL  Yes  No  Strong typing

**Avro** — most common in Kafka ecosystem (Confluent default)
**Protobuf** — growing adoption, better for polyglot environments
**JSON Schema** — easiest to start, worst performance

---

## Avro Schema Example

```json
{
  "type": "record",
  "name": "OrderPlaced",
  "namespace": "com.acme.orders",
  "fields": [
    {"name": "order_id",    "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount",      "type": "double"},
    {"name": "currency",    "type": "string",  "default": "USD"},
    {"name": "placed_at",   "type": "long",    "logicalType": "timestamp-millis"},
    {"name": "metadata",    "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}
```

---

## Schema Evolution and Compatibility

Schema Registry enforces compatibility rules:

 Mode  Allowed Changes
--------
 BACKWARD  Add optional fields, remove optional fields
 FORWARD  Remove optional fields, add optional fields
 FULL  Both backward and forward compatible
 NONE  Any change allowed (dangerous)

Compatibility check on `schema.registry.url` before producing:

```bash
curl -X POST http://schema-registry:8081/compatibility/subjects/orders-value/versions/latest \
  -H "Content-Type: application/json" \
  -d '{"schema": "..."}'
# → {"is_compatible": true}
```

---

## Schema Registry Workflow

```
Producer
  │  (new schema version)
  ▼
Schema Registry
  │  → Check compatibility with latest version
  │  → If compatible: assign schema ID
  │  → If not: reject with error
  ▼
Kafka (message header contains schema ID, not full schema)

Consumer
  │  (receives schema ID in message header)
  ▼
Schema Registry  (fetch schema by ID, cache locally)
  │
  ▼
Deserialize correctly
```

---

## Tiered Storage

**Problem:** Keeping months of data on broker disks is expensive.

**Solution:** Kafka tiered storage offloads older log segments to object storage.

```
Hot tier (broker SSDs):
  Last 7 days  ←── producers write here, consumers read mostly here

Cold tier (S3 / GCS / Azure Blob):
  Last 12 months  ←── older segments, read on demand
```

Benefits:
- Dramatically reduce broker storage costs
- Unlimited retention without adding brokers
- Consumers can replay historical data transparently

Supported by: Confluent Cloud, Amazon MSK, Redpanda

---

## Topic Lifecycle Management

Production topic governance:

1. **Naming conventions** — `<env>.<domain>.<entity>.<event>` e.g. `prod.orders.order.placed`
2. **Owner registration** — each topic has a declared owner team
3. **Retention policies** — set retention.ms and retention.bytes explicitly
4. **Schema enforcement** — all production topics must have a registered schema
5. **Deprecation process** — topics are deprecated before deletion (tombstone period)
6. **Audit** — track topic creation/deletion via changelog

---

## Cross-Cluster Replication — MirrorMaker 2

**MirrorMaker 2 (MM2)** is the built-in replication tool:

```
Source Cluster (us-east-1)
  Topic: orders
     │
     ▼  (MM2 consumer + producer)
Target Cluster (eu-west-1)
  Topic: us-east-1.orders   ← remote topic prefix
```

Features:
- Offset translation (source offsets → target offsets)
- Consumer group offset sync
- Topic configuration sync
- Heartbeat and checkpointing topics

---

## MirrorMaker 2 Configuration

```properties
clusters=source, target

source.bootstrap.servers=kafka-source:9092
target.bootstrap.servers=kafka-target:9092

source->target.enabled=true
source->target.topics=orders, payments, users

replication.factor=3
emit.heartbeats.enabled=true
emit.checkpoints.enabled=true
```

---

## Multi-Region Streaming Architectures

**Active-Active** — both regions produce and consume:
```
Region A ←──── MirrorMaker 2 ────► Region B
  (producers)                       (producers)
  (consumers)                       (consumers)
  ← local reads/writes →          ← local reads/writes →
```

**Active-Passive** — one region is primary:
```
Primary (Active) ──── MirrorMaker 2 ───► DR (Passive)
  (all traffic)                            (standby/read)
```

Trade-offs: active-active has lower latency for global users but requires conflict resolution.

---

## Module 3 Summary

- Partition count = max parallelism; choose based on target throughput
- Key selection is critical — bad keys cause hot partitions
- Use deletion for event history, compaction for state
- Avro and Protobuf are preferred for production; register all schemas
- Schema evolution must follow compatibility rules (prefer BACKWARD)
- Tiered storage enables unlimited retention at reduced cost
- MirrorMaker 2 replicates topics across clusters with offset translation
- Multi-region: active-active for availability, active-passive for DR

---

## What's Next

**Module 4 — Stream Processing with Kafka Streams & ksqlDB**

- Stateless and stateful transformations
- Joins, aggregations, and windowing
- State stores, fault tolerance, and scaling
- Event-driven AI pipelines

---

## Lab Preview — Lab 3

**Create and Benchmark High-Throughput Topics**

You will:
1. Create topics with different partition/replication configurations
2. Produce events with different keys and observe partition distribution
3. Identify and fix a hot partition scenario
4. Register a schema and observe compatibility enforcement
5. Benchmark throughput using `kafka-producer-perf-test.sh`

Environment: Docker Compose Kafka + Schema Registry
Time: 60 minutes

---

