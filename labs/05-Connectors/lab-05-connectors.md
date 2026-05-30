# Lab 5 — Deploy and Tune Source and Sink Connectors

**Module:** 5 — Connectors, Pipelines & Integrations
**Duration:** 60–75 minutes
**Difficulty:** Intermediate

---

## Objectives

By the end of this lab you will be able to:

- Deploy a JDBC source connector reading from PostgreSQL
- Deploy an S3 (MinIO) sink connector writing Parquet files
- Inject bad records and observe Dead Letter Queue behavior
- Tune connector task counts and observe throughput impact
- Simulate a connector failure and verify offset resume
- Use the Kafka Connect REST API for operations

---

## Prerequisites

- Docker Compose cluster with Kafka Connect, PostgreSQL, MinIO
- `curl` and `jq`
- Python 3.9+ with `confluent-kafka`, `psycopg2`

---

## Lab Environment

```bash
# Start full stack
docker compose --profile connect up -d

# Verify services
docker compose ps
curl http://localhost:8083/connectors           # Connect REST API
curl http://localhost:9000/minio/health/live    # MinIO
```

---

## Exercise 1 — Prepare Source Data (PostgreSQL)

### 1.1 Create tables and seed data

```bash
docker exec -it postgres psql -U kafka_user -d orders_db
```

```sql
-- Create orders table with CDC-friendly columns
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create index for efficient CDC polling
CREATE INDEX idx_orders_updated_at ON orders(updated_at);

-- Seed initial data
INSERT INTO orders (order_id, customer_id, amount, status)
SELECT
    'order-' || i,
    'customer-' || (i % 100),
    round((random() * 990 + 10)::numeric, 2),
    CASE WHEN random() > 0.3 THEN 'COMPLETED' ELSE 'PENDING' END
FROM generate_series(1, 500) AS t(i);

SELECT COUNT(*) FROM orders;
\q
```

---

## Exercise 2 — Deploy JDBC Source Connector

### 2.1 Deploy the connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-orders-source",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "connection.url": "jdbc:postgresql://postgres:5432/orders_db",
      "connection.user": "kafka_user",
      "connection.password": "kafka_password",
      "table.whitelist": "orders",
      "mode": "timestamp+incrementing",
      "timestamp.column.name": "updated_at",
      "incrementing.column.name": "id",
      "topic.prefix": "prod.postgres.",
      "poll.interval.ms": "2000",
      "tasks.max": "1",
      "transforms": "addMeta",
      "transforms.addMeta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.addMeta.static.field": "source_table",
      "transforms.addMeta.static.value": "orders"
    }
  }' | jq .
```

### 2.2 Verify connector status

```bash
# Check status
curl http://localhost:8083/connectors/postgres-orders-source/status | jq .

# Check tasks
curl http://localhost:8083/connectors/postgres-orders-source/tasks | jq .
```

### 2.3 Verify events in Kafka

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prod.postgres.orders \
  --from-beginning \
  --max-messages 5 \
  --property print.key=true
```

### 2.4 Insert new rows and observe CDC

```bash
# Insert 50 new orders while consumer watches
docker exec postgres psql -U kafka_user -d orders_db -c "
INSERT INTO orders (order_id, customer_id, amount, status)
SELECT 'new-order-' || i, 'customer-' || (i % 50), random() * 500, 'PENDING'
FROM generate_series(501, 550) AS t(i);"

# Observe new events flowing into Kafka
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prod.postgres.orders \
  --group lab5-observer \
  --timeout-ms 10000
```

**Questions:**
1. How many events were read on the first poll?
2. How long after INSERT did the events appear in Kafka?
3. What fields are in each event?

---

## Exercise 3 — Deploy S3 Sink Connector (MinIO)

### 3.1 Create MinIO bucket

```bash
# MinIO web UI: http://localhost:9001 (admin/password)
# Or via CLI:
docker exec minio mc alias set local http://localhost:9000 admin password
docker exec minio mc mb local/kafka-data-lake
```

### 3.2 Deploy the S3 sink connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders-s3-sink",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "2",
      "topics": "prod.postgres.orders",
      "s3.region": "us-east-1",
      "s3.bucket.name": "kafka-data-lake",
      "s3.part.size": "5242880",
      "store.url": "http://minio:9000",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "flush.size": "100",
      "rotate.interval.ms": "30000",
      "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
      "path.format": "'\''year'\''=YYYY/'\''month'\''=MM/'\''day'\''=dd/'\''hour'\''=HH",
      "locale": "en_US",
      "timezone": "UTC",
      "timestamp.extractor": "RecordField",
      "timestamp.field": "updated_at",
      "s3.credentials.provider.class": "com.amazonaws.auth.AWSStaticCredentialsProvider",
      "s3.credentials.provider.aws.access.key.id": "admin",
      "s3.credentials.provider.aws.secret.access.key": "password"
    }
  }' | jq .
```

### 3.3 Verify files in MinIO

```bash
# Wait ~30 seconds for flush
sleep 35

# List files written to MinIO
docker exec minio mc ls --recursive local/kafka-data-lake/
```

**Questions:**
1. What is the path structure of the files?
2. What determines when a new file is created?
3. How many records are in each file?

---

## Exercise 4 — Error Handling and DLQ

### 4.1 Deploy a connector with DLQ configured

```bash
curl -X PUT http://localhost:8083/connectors/orders-s3-sink/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "2",
    "topics": "prod.postgres.orders",
    "s3.region": "us-east-1",
    "s3.bucket.name": "kafka-data-lake",
    "store.url": "http://minio:9000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "flush.size": "100",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "errors.deadletterqueue.topic.name": "orders-dlq",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "errors.deadletterqueue.context.headers.enable": "true",
    "s3.credentials.provider.class": "com.amazonaws.auth.AWSStaticCredentialsProvider",
    "s3.credentials.provider.aws.access.key.id": "admin",
    "s3.credentials.provider.aws.secret.access.key": "password"
  }' | jq .
```

### 4.2 Inject malformed records

```python
# inject_bad_records.py
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

bad_records = [
    b'this is not json at all!',
    b'{"broken": true, "missing_closing_brace"',
    b'null',
    b'{"order_id": null, "amount": "not-a-number"}',
]

for i, record in enumerate(bad_records):
    producer.produce(
        topic='prod.postgres.orders',
        key=f'bad-key-{i}'.encode(),
        value=record
    )
    print(f"Injected bad record {i}: {record[:40]}")

producer.flush()
print("Injected 4 bad records")
```

```bash
python inject_bad_records.py
sleep 10

# Check DLQ
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning \
  --property print.headers=true \
  --property print.key=true \
  --max-messages 4
```

**Questions:**
1. What headers are attached to DLQ messages?
2. What error information is provided?
3. Without `errors.tolerance=all`, what would happen when a bad record arrives?

---

## Exercise 5 — Connector Failure and Offset Resume

### 5.1 Note current topic offsets

```bash
docker exec kafka-1 kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic prod.postgres.orders
```

### 5.2 Stop the source connector

```bash
curl -X PUT http://localhost:8083/connectors/postgres-orders-source/pause
curl http://localhost:8083/connectors/postgres-orders-source/status | jq '.connector.state'
```

### 5.3 Insert rows while connector is paused

```bash
docker exec postgres psql -U kafka_user -d orders_db -c "
INSERT INTO orders (order_id, customer_id, amount)
SELECT 'paused-order-' || i, 'customer-' || i, 100
FROM generate_series(1, 100) AS t(i);"

echo "Inserted 100 rows while connector was paused"
```

### 5.4 Resume connector and verify catch-up

```bash
curl -X PUT http://localhost:8083/connectors/postgres-orders-source/resume

# Watch events arriving in Kafka
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prod.postgres.orders \
  --group lab5-catchup \
  --timeout-ms 15000 \
  --property print.key=true \
  | grep "paused-order"
```

**Questions:**
1. Did the connector pick up the rows inserted while paused?
2. How did Kafka Connect know where to resume?
3. What would happen if you deleted and recreated the connector?

---

## Exercise 6 — Task Scaling and Throughput

### 6.1 Measure single-task throughput

```bash
# Insert 5000 rows
docker exec postgres psql -U kafka_user -d orders_db -c "
INSERT INTO orders (order_id, customer_id, amount)
SELECT 'bulk-order-' || i, 'customer-' || (i % 1000), random() * 1000
FROM generate_series(1, 5000) AS t(i);"

time docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group connect-postgres-orders-source 2>/dev/null
```

### 6.2 Increase task count

```bash
curl -X PUT http://localhost:8083/connectors/postgres-orders-source/config \
  -H "Content-Type: application/json" \
  -d '{ ... existing config ..., "tasks.max": "3" }'
```

**Note:** JDBC connector with `incrementing` mode typically supports only 1 task per table. Observe that increasing `tasks.max` may not linearly scale for single-table polling connectors.

**Questions:**
1. Did increasing `tasks.max` improve throughput for the JDBC connector?
2. For what type of connectors does `tasks.max` provide linear scaling?
3. What is the difference between connector parallelism and topic parallelism?

---

## Lab Summary

You have deployed and operated:

- A JDBC source connector ingesting PostgreSQL CDC into Kafka
- An S3/MinIO sink connector writing time-partitioned JSON files
- DLQ configuration for error handling (injected and observed bad records)
- Connector pause/resume with offset tracking verification
- Task scaling analysis

**Key takeaway:** Kafka Connect removes the need to write custom integration code. Understanding offset management, error handling, and task parallelism lets you build reliable, production-grade data pipelines.

---

## Review Questions

1. Where does a JDBC source connector store its read position (offset)?
2. What happens to events that fail processing when `errors.tolerance=all`?
3. What determines how many tasks a connector can use?
4. How would you monitor connector health in production?

---

## What's Next

**Module 6** covers reliability, performance tuning, and cluster operations — you'll stress-test a Kafka cluster and analyze rebalance behavior.

