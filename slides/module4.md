# Module 4 — Stream Processing with Kafka Streams & ksqlDB

Elephant Scale

---

## Module 4 Agenda

- Stateless vs stateful transformations
- Joins, aggregations, and windowing
- Handling out-of-order and late-arriving data
- State stores and fault tolerance
- Scaling stream processing applications
- Monitoring and debugging stream topologies
- Event-driven AI and anomaly detection pipelines

---

## Stream Processing Fundamentals

**Stream processing** = transforming, enriching, or aggregating data **as it arrives**.

Two paradigms:
- **Stateless** — each record is processed independently
- **Stateful** — processing requires memory of past records

```
Stateless:  [event] → transform → [result]
Stateful:   [event1, event2, event3] → aggregate → [result]
                └─ requires STATE STORE ──────────┘
```

---

## Kafka Streams Topology

A Kafka Streams application is a **topology** of processors:

```
Source Topic (orders)
      │
  [Filter]  ← stateless
      │
  [Map]     ← stateless (transform records)
      │
  [GroupBy] ← prepare for aggregation
      │
  [Aggregate] ← stateful (requires state store)
      │
Sink Topic (order-totals)
```

Each node in the topology processes records in the same thread.

---

## Stateless Transformations

```java
KStream<String, Order> orders = builder.stream("orders");

// Filter: only completed orders
KStream<String, Order> completed = orders
    .filter((key, order) -> order.getStatus().equals("COMPLETED"));

// Map: extract amount only
KStream<String, Double> amounts = completed
    .mapValues(order -> order.getAmount());

// FlatMap: one order → multiple line items
KStream<String, LineItem> items = orders
    .flatMapValues(order -> order.getLineItems());

// Branch: split into streams
Map<String, KStream<String, Order>> branches = orders
    .split()
    .branch((k, v) -> v.getAmount() > 1000, Branched.as("high"))
    .branch((k, v) -> v.getAmount() <= 1000, Branched.as("low"))
    .defaultBranch(Branched.as("default"));
```

---

## Stateful Transformations — Aggregations

```java
KTable<String, Long> orderCountByCustomer = orders
    .groupBy((key, order) -> order.getCustomerId())   // rekey
    .count(Materialized.as("order-counts-store"));

KTable<String, Double> totalByCustomer = orders
    .groupBy((key, order) -> order.getCustomerId())
    .aggregate(
        () -> 0.0,                                    // initializer
        (customerId, order, total) -> total + order.getAmount(),  // aggregator
        Materialized.as("order-totals-store")
    );
```

Results materialize as a **KTable** — current state per key, backed by a changelog topic.

---

## KStream vs KTable

 Concept  KStream  KTable
--------
 Represents  Unbounded stream of events  Current state per key (changelog)
 Semantics  Append  Upsert (latest value per key)
 Source  `builder.stream()`  `builder.table()`
 Backed by  Source topic  State store + changelog topic
 Use for  Events (orders, clicks)  State (user profiles, balances)

---

## Joins in Kafka Streams

Three join types:

**KStream-KStream join** (windowed, both sides moving):
```java
KStream<String, EnrichedOrder> enriched = orders.join(
    payments,
    (order, payment) -> new EnrichedOrder(order, payment),
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
);
```

**KStream-KTable join** (enrich stream with current state):
```java
KStream<String, EnrichedOrder> enriched = orders.join(
    customerTable,
    (order, customer) -> new EnrichedOrder(order, customer)
);
```

**KTable-KTable join** (join two state tables):
```java
KTable<String, Combined> combined = tableA.join(tableB, (a, b) -> merge(a, b));
```

---

## Windowing

Windows group events by **time** for aggregation.

**Tumbling window** — fixed size, non-overlapping:
```
[0:00–1:00] [1:00–2:00] [2:00–3:00]
```

**Hopping window** — fixed size, overlapping:
```
[0:00–1:00]
      [0:30–1:30]
            [1:00–2:00]
```

**Sliding window** — spans a fixed duration around each event:
```java
SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30))
```

**Session window** — groups events by activity gap:
```
[events] --- (gap > 30min) --- [next session]
```

---

## Event Time vs Processing Time

**Event time** — when the event actually occurred (in the record)
**Processing time** — when Kafka Streams processes the record

```
Event: {order_id: 123, placed_at: 10:00:05, processed_at: 10:01:22}
                         ↑ event time              ↑ processing time
```

Use event time for accurate business aggregations:
```java
builder.stream("orders",
    Consumed.with(keySerde, valueSerde)
            .withTimestampExtractor(new OrderTimestampExtractor())
);
```

---

## Out-of-Order and Late Data

In distributed systems, events **always arrive out of order**.

```
Event times received in processing order:
  10:00:01, 10:00:05, 10:00:03 (late!), 10:00:07, 09:59:58 (very late!)
```

Kafka Streams uses **grace periods**:
```java
TimeWindows.ofSizeAndGrace(
    Duration.ofMinutes(1),   // window size
    Duration.ofSeconds(30)   // grace: accept late events up to 30s after window closes
);
```

Events arriving after the grace period are **dropped** (default) or routed to a late-data topic.

---

## State Stores

State stores are the **memory** of stateful stream processors.

Types:
- **In-memory** — fast, lost on restart
- **Persistent** (RocksDB default) — survives restarts, backed up to Kafka changelog topic
- **Versioned** — stores multiple historical values per key

```java
// Access state store from outside the app (interactive queries)
ReadOnlyKeyValueStore<String, Long> store =
    streams.store(StoreQueryParameters.fromNameAndType(
        "order-counts-store",
        QueryableStoreTypes.keyValueStore()
    ));
Long count = store.get("customer-123");
```

---

## Fault Tolerance and State Recovery

If a Kafka Streams instance crashes:

```
1. State store is backed up to changelog topic (Kafka topic)
   orders-count-store-changelog → Kafka
2. On restart: restore state from changelog topic
3. Resume processing from last committed offset
```

Standby replicas (optional):
```java
streamsConfig.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
// → A standby instance maintains a warm copy of the state store
// → Faster recovery on failover
```

---

## Scaling Kafka Streams

Scale by **adding instances** of the same application:

```
Application: payment-processor (3 instances)
Topic: orders (6 partitions)

Instance 1: partitions 0, 1
Instance 2: partitions 2, 3
Instance 3: partitions 4, 5
```

Rules:
- Max parallelism = number of partitions
- State stores are partitioned across instances
- Rebalance is triggered when instances join/leave

---

## ksqlDB — SQL for Streams

ksqlDB provides a **persistent SQL layer** over Kafka:

```sql
-- Continuous filter
CREATE STREAM large_orders AS
SELECT * FROM orders WHERE amount > 5000;

-- Windowed aggregation
CREATE TABLE hourly_revenue AS
SELECT
    TIMESTAMPTOSTRING(WINDOWSTART, 'HH:mm') AS hour,
    SUM(amount) AS revenue
FROM orders
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY 1;

-- Stream-table join (enrich with customer data)
CREATE STREAM enriched_orders AS
SELECT o.*, c.name, c.tier
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id;
```

---

## ksqlDB Architecture

```
ksqlDB Server
    │  (runs Kafka Streams internally)
    │
    ├── REST API  ← for ad-hoc queries, DDL
    ├── CLI       ← interactive ksql>
    └── Pull queries  ← point-in-time state lookups
            │
    Kafka Topics (source + sink)
            │
    Schema Registry (optional)
```

ksqlDB is **Kafka-native** — results are always Kafka topics.

---

## Event-Driven AI Pipelines

Kafka enables **real-time ML inference pipelines**:

```
Raw Events (sensor data)
      │
  [Feature Extraction]  ← Kafka Streams
      │
  [Feature Store Topic]
      │
  [ML Model Serving]  ← Kafka Streams or external
      │
  [Inference Results Topic]
      │
  [Downstream Actions]  (alerts, recommendations, routing)
```

---

## Anomaly Detection Pattern

```java
// Kafka Streams: sliding window anomaly detection
KStream<String, SensorReading> readings = builder.stream("sensor-data");

KTable<Windowed<String>, Double> avgByDevice = readings
    .groupByKey()
    .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(
        Duration.ofMinutes(5), Duration.ofSeconds(30)))
    .aggregate(
        RunningAverage::new,
        (key, reading, avg) -> avg.update(reading.getValue()),
        Materialized.with(Serdes.String(), runningAvgSerde)
    );

// Detect readings > 3 standard deviations from window mean
readings.join(avgByDevice, ...)
    .filter((key, pair) -> isAnomaly(pair))
    .to("anomalies");
```

---

## Debugging Stream Topologies

Print the topology description:
```java
System.out.println(builder.build().describe());
```

Output:
```
Topologies:
  Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [orders])
      --> KSTREAM-FILTER-0000000001
    Processor: KSTREAM-FILTER-0000000001 (stores: [])
      --> KSTREAM-SINK-0000000002
    Sink: KSTREAM-SINK-0000000002 (topic: completed-orders)
```

Monitor with:
- JMX metrics (stream thread, task, state store metrics)
- Kafka Streams built-in health check endpoint

---

## Module 4 Summary

- Stateless transformations operate record-by-record; stateful require state stores
- Kafka Streams provides a DSL for filter, map, join, aggregate, and windowing
- Windows: tumbling (non-overlapping), hopping (overlapping), sliding, session
- Always use event time for business-correct aggregations
- Grace periods handle late-arriving events; state stores use RocksDB + changelog
- Scale by adding instances; max parallelism = partition count
- ksqlDB provides SQL over streams — results are always Kafka topics
- Kafka Streams is the backbone of real-time AI inference pipelines

---

## What's Next

**Module 5 — Connectors, Pipelines & Integrations**

- Kafka Connect deep dive
- Source and sink connector configuration
- Custom connector development
- Integration patterns: S3, Elasticsearch, Flink, Spark

---

## Lab Preview — Lab 4

**Build an Anomaly Detection Pipeline with Kafka Streams**

You will:
1. Write a Kafka Streams application that computes rolling averages
2. Detect anomalies using sliding window statistics
3. Route anomalous events to an `anomalies` topic
4. Query state stores interactively
5. Observe topology under load

Environment: Docker Compose Kafka + Java 17
Time: 60 minutes

---

