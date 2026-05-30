# Module 5 — Connectors, Pipelines & Integrations

Elephant Scale

---

## Module 5 Agenda

- Kafka Connect deep dive
- Source and sink connectors
- Offset management, retries, and error handling
- Custom connector development
- Integration patterns: S3, Elasticsearch, Flink, Spark, NiFi
- Stream-to-batch handoff
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
- Built-in offset management and exactly-once delivery
- REST API for deployment and monitoring
- Horizontal scaling (connector tasks)
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

Workers are **stateless** — all state is in Kafka internal topics.

---

## Kafka Connect Internals

Three internal Kafka topics manage Connect cluster state:

 Topic  Purpose
--------
 `connect-configs`  Connector and task configurations
 `connect-offsets`  Source connector read positions
 `connect-status`  Connector and task status

Benefits:
- Workers are completely stateless
- Full state survives cluster restarts
- Any worker can pick up any task

---

## Source Connector — Example (JDBC)

```json
{
  "name": "postgres-orders-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/orders_db",
    "connection.user": "kafka_user",
    "connection.password": "${file:/opt/kafka/secrets:db_password}",
    "table.whitelist": "orders,order_items",
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

## Sink Connector — Example (S3)

```json
{
  "name": "orders-s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "4",
    "topics": "prod.orders.orders",
    "s3.region": "us-east-1",
    "s3.bucket.name": "my-data-lake",
    "s3.part.size": "67108864",
    "flush.size": "10000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "schema.compatibility": "FULL",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "locale": "en_US",
    "timezone": "UTC"
  }
}
```

---

## Offset Management in Source Connectors

Source connectors store their read position in `connect-offsets`:

```
JDBC connector offset:
  {"table": "orders"} → {"incrementing": 12345, "timestamp": 1716000000000}

File connector offset:
  {"filename": "/data/events.log"} → {"position": 40960}
```

On restart: connector resumes from last stored offset.

For **exactly-once** source connectors (Kafka 3.3+):
- Offsets are committed atomically with the Kafka produce transaction
- No duplicates even on connector worker failure

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

Failed records go to the DLQ with headers explaining the error:

```
Header: connect.errors.exception.class.name = org.apache.kafka.connect.errors.DataException
Header: connect.errors.exception.message = Failed to deserialize...
Header: connect.errors.topic = prod.orders.orders
Header: connect.errors.partition = 2
Header: connect.errors.offset = 42
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

Retry strategy: **exponential backoff** up to `errors.retry.delay.max.ms`.

For transient failures (network timeouts, temporary unavailability) — retries are essential.
For persistent failures (schema mismatch, corrupted records) — send to DLQ immediately.

---

## Custom Connector Development

Implement two interfaces:

```java
// Source connector
public class MySourceConnector extends SourceConnector {
    public void start(Map<String, String> props) { ... }
    public Class<? extends Task> taskClass() { return MySourceTask.class; }
    public List<Map<String, String>> taskConfigs(int maxTasks) { ... }
    public void stop() { ... }
    public ConfigDef config() { ... }
}

public class MySourceTask extends SourceTask {
    public List<SourceRecord> poll() throws InterruptedException {
        // Read from source, return SourceRecord list
        return records;
    }
    public void commitRecord(SourceRecord record) { ... }
}
```

---

## Kafka ↔ S3 / Data Lake Pattern

```
Kafka (hot tier)
    │  (S3 Sink Connector, Parquet format)
    ▼
S3 Data Lake
    │
    ├── AWS Athena / Presto (ad-hoc SQL)
    ├── AWS Glue / Apache Hive (ETL)
    └── Apache Spark (batch processing)
```

Best practices:
- Use Parquet or Avro format for efficient columnar storage
- Partition by date/hour in S3 path
- Use Schema Registry to ensure schema consistency
- Set `flush.size` and rotation interval to control file sizes

---

## Kafka ↔ Elasticsearch / OpenSearch Pattern

```
Kafka (events topic)
    │  (ES Sink Connector)
    ▼
Elasticsearch / OpenSearch
    │
    ├── Kibana / OpenSearch Dashboards (visualization)
    ├── Full-text search
    └── Log analytics
```

```json
{
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "topics": "logs,events",
  "connection.url": "http://elasticsearch:9200",
  "type.name": "_doc",
  "key.ignore": "true",
  "schema.ignore": "true",
  "behavior.on.malformed.documents": "warn"
}
```

---

## Kafka ↔ Apache Flink

Flink natively reads from and writes to Kafka:

```java
// Flink source
KafkaSource<Order> source = KafkaSource.<Order>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("orders")
    .setGroupId("flink-orders")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new OrderDeserializer())
    .build();

// Flink sink
KafkaSink<EnrichedOrder> sink = KafkaSink.<EnrichedOrder>builder()
    .setBootstrapServers("kafka:9092")
    .setRecordSerializer(new EnrichedOrderSerializer("enriched-orders"))
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .build();
```

---

## Kafka ↔ Apache Spark

Spark Structured Streaming reads Kafka as a streaming source:

```python
# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON, aggregate
orders = df.select(from_json(col("value").cast("string"), schema).alias("order"))
aggregated = orders.groupBy(window("order.placed_at", "1 hour"), "order.region") \
    .agg(sum("order.amount").alias("revenue"))

# Write back to Kafka
aggregated.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "hourly-revenue") \
    .start()
```

---

## Stream-to-Batch Handoff

Many pipelines need both real-time and batch processing:

```
Kafka (real-time)
    │
    ├── Stream consumers (immediate action)
    │
    └── S3 Sink Connector → S3 (batch files)
              │
              └── Spark/Hive batch job (daily aggregation)
```

**Lambda architecture** — dual path (real-time + batch), merge results.
**Kappa architecture** — single Kafka path, replay historical data for batch.

Kappa is preferred when:
- Historical data is in Kafka (with sufficient retention)
- Stream processing logic can reproduce batch results

---

## Backpressure Management

When consumers fall behind:

```
Producer throughput:   100,000 msg/sec
Consumer throughput:    80,000 msg/sec
Consumer lag:          growing → 10M messages → 100M messages
```

Solutions:
1. **Scale consumers** — add more consumer instances / tasks
2. **Tune `fetch.max.bytes` and `max.poll.records`** — process more per poll
3. **Tune connector `tasks.max`** — add more sink connector tasks
4. **Circuit breaker** — pause connector if sink is unavailable
5. **Priority lanes** — use separate topics for high-priority events

---

## Enterprise Integration Patterns

**Event-Carried State Transfer** — include all needed state in the event (avoid joins):
```json
{"order_id": "123", "customer_name": "Alice", "customer_email": "alice@acme.com", ...}
```

**Event Notification** — minimal event, consumer fetches details:
```json
{"order_id": "123", "status": "completed"}
```

**Saga** — coordinate distributed transactions via events:
```
OrderPlaced → PaymentRequested → PaymentCompleted → OrderFulfilled
```

**Outbox Pattern** — write event to DB outbox table + CDC connector (avoids dual write):
```
App → DB (order row + outbox event) → Debezium CDC → Kafka
```

---

## Module 5 Summary

- Kafka Connect provides 700+ connectors with built-in offset management
- Internal topics (configs, offsets, status) make Connect workers stateless
- Error handling: configure retries, tolerance level, and DLQ for production
- S3, Elasticsearch, Flink, Spark all have mature Kafka integrations
- Stream-to-batch: use S3 sink for data lake, consider Kappa architecture
- Backpressure: scale tasks, tune fetch parameters, implement circuit breakers
- Enterprise patterns: outbox, saga, event-carried state transfer

---

## What's Next

**Module 6 — Reliability, Scaling & Performance**

- Throughput tuning parameters
- Consumer lag detection and remediation
- Cluster scaling strategies
- Disaster recovery and failover patterns

---

## Lab Preview — Lab 5

**Deploy and Tune Source and Sink Connectors**

You will:
1. Deploy a JDBC source connector reading from PostgreSQL
2. Deploy an S3 sink connector writing Parquet files
3. Inject a bad record and observe DLQ behavior
4. Tune connector tasks and observe throughput impact
5. Test connector restart and offset resume

Environment: Docker Compose (Kafka, PostgreSQL, MinIO/S3)
Time: 60 minutes

---

