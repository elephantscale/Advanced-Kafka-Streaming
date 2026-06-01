# Module 4 — Connectors, Pipelines & Integrations

Elephant Scale

---

## Module 4 Agenda

- Why Kafka Connect exists
- Connect architecture and internal topics
- Source connectors, sink connectors, offset management
- Error handling, retries, and Dead Letter Queues
- Custom connector development
- Integration patterns: S3, Elasticsearch, Flink, Spark, Iceberg/lakehouse
- Stream-to-batch handoff and the lakehouse path
- Backpressure management
- Enterprise integration patterns

---

## Why Kafka Connect?

Moving data between systems is **the hardest part of streaming**.

Without Kafka Connect:
- Custom code for every integration
- Reinventing offset management, error handling, restarts
- No standard for connector configuration or monitoring

With Kafka Connect:
- 700+ production-ready connectors
- Built-in offset management and exactly-once delivery (Kafka 3.3+)
- REST API for deployment and monitoring
- Horizontal scaling via connector tasks
- Schema Registry integration

---

## Kafka Connect Architecture

```
External Source          Kafka Connect Workers          Kafka
─────────────           ────────────────────           ─────
PostgreSQL  ──────────► Source Connector Task ──────► orders topic
MySQL       ──────────► Source Connector Task ──────► users topic

Kafka                   Kafka Connect Workers          Sinks
─────                   ────────────────────           ─────
events topic ─────────► Sink Connector Task ─────────► Elasticsearch
metrics topic ────────► Sink Connector Task ─────────► S3 / Data Lake
```

Workers are **stateless** — all state is stored in Kafka internal topics.

---

## Connect Internal Topics

| Topic | Purpose |
|-------|---------|
| `connect-configs` | Connector and task configurations |
| `connect-offsets` | Source connector read positions |
| `connect-status` | Connector and task status |

Benefits:
- Workers are completely stateless — any worker can pick up any task
- Full state survives cluster restarts and worker failures

---

## Source Connector — JDBC Example

```json
{
  "name": "postgres-orders-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/orders_db",
    "connection.user": "kafka_user",
    "connection.password": "${file:/opt/kafka/secrets:db_password}",
    "table.whitelist": "orders",
    "mode": "timestamp+incrementing",
    "timestamp.column.name": "updated_at",
    "incrementing.column.name": "id",
    "topic.prefix": "prod.orders.",
    "poll.interval.ms": "1000",
    "tasks.max": "3"
  }
}
```

---

## Sink Connector — S3 Example

```json
{
  "name": "orders-s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "4",
    "topics": "prod.orders.orders",
    "s3.bucket.name": "my-data-lake",
    "flush.size": "10000",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "timezone": "UTC"
  }
}
```

---

## Offset Management

Source connectors store their read position in `connect-offsets`:

```
JDBC connector offset:
  {"table": "orders"} → {"incrementing": 12345, "timestamp": 1716000000000}
```

On restart: connector **resumes from last stored offset** — no duplicates, no data loss.

For **exactly-once** source connectors (GA since Kafka 3.3, standard in Kafka 4):
- Offsets are committed atomically with the Kafka produce transaction
- No duplicates even on connector worker failure
- Enable with `exactly.once.source.support=enabled` on the Connect worker

---

## Error Handling and Dead Letter Queues

```json
{
  "errors.tolerance": "all",
  "errors.log.enable": true,
  "errors.log.include.messages": true,
  "errors.deadletterqueue.topic.name": "orders-dlq",
  "errors.deadletterqueue.topic.replication.factor": 3,
  "errors.deadletterqueue.context.headers.enable": true
}
```

Failed records go to the DLQ with error headers:
```
connect.errors.exception.class.name = DataException
connect.errors.topic = prod.orders.orders
connect.errors.offset = 42
```

---

## Retry Configuration

```json
{
  "retry.backoff.ms": "500",
  "max.retries": "10",
  "errors.retry.timeout": "300000",
  "errors.retry.delay.max.ms": "60000"
}
```

- **Transient failures** (network timeouts, unavailability) → retry with exponential backoff
- **Persistent failures** (schema mismatch, corrupt records) → send to DLQ immediately

---

## Kafka ↔ Flink

Flink natively reads from and writes to Kafka:

```java
KafkaSource<Order> source = KafkaSource.<Order>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("orders")
    .setGroupId("flink-orders")
    .setValueOnlyDeserializer(new OrderDeserializer())
    .build();

KafkaSink<EnrichedOrder> sink = KafkaSink.<EnrichedOrder>builder()
    .setBootstrapServers("kafka:9092")
    .setRecordSerializer(new EnrichedOrderSerializer("enriched-orders"))
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .build();
```

---

## Kafka ↔ Spark

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .load()

aggregated = df \
    .select(from_json(col("value").cast("string"), schema).alias("order")) \
    .groupBy(window("order.placed_at", "1 hour"), "order.region") \
    .agg(sum("order.amount").alias("revenue"))

aggregated.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "hourly-revenue") \
    .start()
```

---

## Stream-to-Batch Handoff

```
Kafka (real-time)
    │
    ├── Stream consumers (immediate action)
    │
    └── S3 Sink Connector → S3 (batch files)
              │
              └── Spark / Athena batch job (daily aggregation)
```

- **Lambda architecture** — dual path (real-time + batch), merge results
- **Kappa architecture** — single Kafka path, replay for batch (preferred when retention allows)

**Modern lakehouse path (2026):** instead of landing raw files, sink directly to
**Apache Iceberg** tables — via the Iceberg Sink Connector, or Confluent **Tableflow**
(Kafka topics materialized as Iceberg/Delta tables). Query from Spark, Flink, Trino, or
Athena with no separate ETL job. Pairs naturally with Tiered Storage.

---

## Backpressure Management

When consumers fall behind producers:

| Root Cause | Fix |
|---|---|
| Too few connector tasks | Increase `tasks.max` |
| Slow sink system | Circuit breaker, async writes |
| Producer burst | Separate high/low-priority topics |
| Consumer processing slow | Scale consumer instances |

---

## Enterprise Integration Patterns

**Outbox Pattern** — eliminates dual-write risk:
```
App → DB (row + outbox event atomically) → Debezium CDC → Kafka
```

**Saga** — distributed transaction coordination via events:
```
OrderPlaced → PaymentRequested → PaymentCompleted → OrderFulfilled
```

**Event-Carried State Transfer** — embed full state in the event (avoid lookups):
```json
{"order_id": "123", "customer_name": "Alice", "amount": 99.99, ...}
```

---

## Module 4 Summary

- Kafka Connect provides 700+ connectors with built-in offset management
- Internal topics make workers completely stateless and fault-tolerant
- Configure `errors.tolerance`, retries, and DLQ for production resilience
- Flink and Spark both have mature native Kafka integrations
- Stream-to-batch: S3 sink + Kappa architecture, increasingly going straight to an Iceberg lakehouse (Tableflow)
- Enterprise patterns: outbox, saga, event-carried state transfer

---

## What's Next

**Module 5 — Reliability, Scaling & Performance**

- Capacity planning and right-sizing
- Expanding Kafka with no data loss
- HA and performance tuning: replication, acks, consumer lag monitoring

---

## Lab Preview — Lab 4

**Deploy and Tune Source and Sink Connectors**

You will:
1. Deploy a JDBC source connector reading from PostgreSQL
2. Deploy an S3/MinIO sink connector writing time-partitioned files
3. Inject bad records and observe Dead Letter Queue behavior
4. Pause, insert rows, resume — verify offset tracking
5. Discuss task scaling limits for JDBC vs file connectors

Environment: Docker Compose (Kafka, PostgreSQL, MinIO, Kafka Connect)
Time: 60–75 minutes

---

