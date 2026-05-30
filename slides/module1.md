# Module 1 — Modern Event-Driven Architecture with Kafka

Elephant Scale

---

## Module 1 Agenda

- Why Kafka is the backbone of real-time systems
- Modern event-driven technology stack
- Data flow walkthrough
- Event streams vs queue semantics
- Integration with external systems
- Enterprise streaming evolution

---

## Why Real-Time Matters

Modern enterprises need to act on data **as it happens**, not hours later.

Use cases that demand real-time streaming:
- Fraud detection (milliseconds to block a transaction)
- IoT sensor telemetry (millions of devices)
- Observability pipelines (logs, metrics, traces)
- Personalization engines (act while the user is still on the page)
- Supply chain event tracking

> Batch processing is dead for latency-sensitive workloads.

---

## What Is Apache Kafka?

Kafka is a **distributed event streaming platform**.

Core capabilities:
- **Publish and subscribe** to streams of events
- **Store** events durably and reliably
- **Process** events as they occur

Key design goals:
- High throughput (millions of events/sec)
- Low latency (milliseconds)
- Fault tolerance (replication)
- Horizontal scalability

---

## Kafka in the Enterprise Landscape

```
Microservices    IoT Devices    Databases    External APIs
      │               │              │              │
      └───────────────┴──────────────┴──────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   Apache Kafka    │
                    │  (Event Backbone) │
                    └─────────┬─────────┘
                              │
      ┌───────────────┬────────┴────────┬───────────────┐
      │               │                 │               │
  Analytics      Data Lake          Microservices    AI/ML
  (Spark/Flink)  (S3/HDFS)          (Consumers)    Pipelines
```

Kafka is the **central nervous system** of the modern data platform.

---

## The Modern Event-Driven Technology Stack

 Component  Role
-----------
 Kafka Core  Durable event log, topic management, replication
 Kafka Connect  Data integration — source and sink connectors
 Kafka Streams  Stream processing library (JVM)
 ksqlDB  SQL interface for stream processing
 Schema Registry  Schema management (Avro, Protobuf, JSON Schema)
 Monitoring Layer  Prometheus, Grafana, Burrow, Cruise Control
 Security Layer  TLS, SASL, ACLs, RBAC

---

## Kafka Core Concepts

**Topic** — a named, ordered, immutable log of events

**Partition** — a topic is split into partitions for parallelism

**Offset** — position of an event within a partition

**Producer** — writes events to topics

**Consumer** — reads events from topics

**Consumer Group** — a set of consumers that share the work of reading a topic

**Broker** — a Kafka server that stores and serves partitions

---

## Data Flow Walkthrough

```
Producers
  │  (write events)
  ▼
┌────────────────────────────┐
│  Kafka Cluster             │
│  ┌──────┐  ┌──────┐        │
│  │ B1   │  │ B2   │  ...   │
│  └──────┘  └──────┘        │
│   Topic partitions         │
└───────────┬────────────────┘
            │  (read events)
            ▼
Consumers / Consumer Groups
            │
            ▼
Analytics │ Data Lake │ Microservices │ AI Pipelines
```

---

## Event Streams vs Queue Semantics

 Property  Message Queue  Kafka Event Stream
----------
 Message deletion  After consumption  Configurable retention
 Multiple consumers  Competing consumers  Each group reads independently
 Replay  Not possible  Yes — seek to any offset
 Ordering  Per-queue  Per-partition
 Durability  Varies  Configurable replication
 Backpressure  Built-in  Consumer controls its own pace

> Kafka supports **both** patterns since Apache Kafka 3.x queue semantics additions.

---

## Topics, Partitions, and Offsets

```
Topic: orders
  Partition 0:  [0] [1] [2] [3] [4] ...
  Partition 1:  [0] [1] [2] [3] ...
  Partition 2:  [0] [1] [2] ...

Consumer Group A (reads all partitions):
  Consumer 1 → Partition 0 (at offset 4)
  Consumer 2 → Partition 1 (at offset 3)
  Consumer 3 → Partition 2 (at offset 2)
```

- Events within a partition are **strictly ordered**
- Events across partitions are **not globally ordered**
- Partition count = maximum consumer group parallelism

---

## Kafka Connect Overview

Kafka Connect provides **pre-built, scalable data pipelines**.

```
Source Systems          Kafka Connect          Kafka Topics
─────────────          ─────────────          ────────────
PostgreSQL    ──────►  Source Connector  ──►  orders
MySQL         ──────►  Source Connector  ──►  users
S3 bucket     ──────►  Source Connector  ──►  files

Kafka Topics          Kafka Connect           Sink Systems
────────────          ─────────────          ─────────────
analytics   ──────►   Sink Connector   ──►  Elasticsearch
events      ──────►   Sink Connector   ──►  S3 / Data Lake
metrics     ──────►   Sink Connector   ──►  ClickHouse
```

700+ connectors available on Confluent Hub.

---

## Kafka Streams Overview

Kafka Streams is a **Java/Scala library** for stream processing.

Key features:
- No separate cluster required — runs inside your application
- Exactly-once processing semantics
- Stateful operations (joins, aggregations, windowing)
- Interactive queries (expose state to other services)
- Fault-tolerant state backed by changelog topics

```java
KStream<String, Order> orders = builder.stream("orders");
KTable<String, Long> orderCounts = orders
    .groupByKey()
    .count();
orderCounts.toStream().to("order-counts");
```

---

## ksqlDB Overview

ksqlDB is a **SQL engine for stream processing** built on Kafka Streams.

```sql
-- Create a stream from a topic
CREATE STREAM orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    amount DOUBLE
) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');

-- Continuous query: high-value orders
CREATE TABLE high_value_orders AS
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 10000;
```

Queries run **continuously** — results update as new events arrive.

---

## Schema Registry

Schema Registry manages and enforces event schemas.

```
Producer → [Serialize with Schema] → Kafka
                    │
              Schema Registry
              (stores schema,
               assigns schema ID)
                    │
Consumer → [Deserialize with Schema ID] → Application
```

Benefits:
- Schema evolution without breaking consumers
- Compatibility checks (BACKWARD, FORWARD, FULL)
- Centralized schema governance

---

## Integration with External Systems

Kafka integrates with the full data ecosystem:

 System  Integration Pattern
---------
 Apache Spark  Spark Structured Streaming reads from Kafka
 Apache Flink  Native Kafka source/sink connectors
 REST APIs  Kafka REST Proxy for HTTP-based producers/consumers
 AI/ML Pipelines  Feature stores, model serving, inference logging
 Cloud (AWS/GCP/Azure)  MSK, Confluent Cloud, Event Hubs

---

## Streaming-First Application Design

Traditional design:
```
App → Database → Report (batch, hours later)
```

Streaming-first design:
```
App → Kafka → Real-time processor → Immediate action
          └──► Data lake (durable replay)
          └──► ML pipeline (continuous training)
          └──► Microservices (event-driven triggers)
```

Design principle: **events are the source of truth**, not the database.

---

## Enterprise Streaming Patterns

**Event Sourcing** — store all state changes as events, derive state by replaying

**CQRS** — separate read (query) and write (command) models via events

**Saga Pattern** — coordinate long-running distributed transactions via events

**CDC (Change Data Capture)** — stream database changes into Kafka

**Fan-out** — one event triggers multiple independent consumers

---

## Kafka at Scale — Real Numbers

Production deployments at scale:

 Company  Scale
--------
 LinkedIn  7 trillion messages/day
 Uber  1 trillion messages/day
 Netflix  Trillions of events/day
 Cloudflare  Petabytes/day
 Walmart  Billions of events during peak

> Kafka is proven at the most extreme scales on the planet.

---

## Module 1 Summary

- Kafka is the distributed event streaming backbone for modern enterprises
- Core abstractions: topics, partitions, offsets, producers, consumers
- Event streams differ from queues in durability, replay, and multi-consumer support
- The full stack: Kafka Core + Connect + Streams + ksqlDB + Schema Registry
- Integrates with Spark, Flink, REST, AI/ML, and cloud-native systems
- Streaming-first design treats events as the source of truth

---

## What's Next

**Module 2 — Kafka Internals & Cluster Architecture**

We go deep inside the broker:
- Log segments, indexes, and compaction
- KRaft — ZooKeeper-free Kafka
- Producer and consumer internals
- Exactly-once semantics

---

## Lab Preview — Lab 1

**Explore Kafka Cluster Topology and Topic Configuration**

You will:
1. Connect to a running Kafka cluster and inspect broker metadata
2. Create topics with different partition counts and replication factors
3. Examine partition assignment and leader distribution
4. Configure and observe retention policies
5. Use `kafka-topics.sh` and `kafka-consumer-groups.sh`

Environment: Docker Compose Kafka cluster
Time: 45 minutes

---

