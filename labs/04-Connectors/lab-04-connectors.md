# Lab 4 — Deploy and Tune Source and Sink Connectors

- **Module:** 4 — Connectors, Pipelines & Integrations
- **Duration:** 60–75 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

---

## Objectives

By the end of this lab you will be able to:

- Deploy a JDBC source connector reading from PostgreSQL
- Deploy an S3 (MinIO) sink connector writing time-partitioned JSON files
- Configure Dead Letter Queue (DLQ) error handling and inject bad records
- Verify offset tracking through connector pause, insert, and resume
- Tune connector task counts and discuss parallelism limits

---

## Prerequisites

- Docker Compose cluster with Kafka Connect, PostgreSQL, and MinIO
- `curl` and `jq` installed
- Python 3.9+ with `confluent-kafka` and `psycopg2`

---

## Lab Environment

> **Lab environment** — same across all seven labs
>
> - Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free.
> - Local **Docker Compose** cluster. The main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
> - Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster. If a step reports one unavailable, treat it as instructor-led.
> - Full setup and prerequisites: `labs/SETUP.md`.
> - **Python exercises:** activate the virtualenv first — `source .venv/bin/activate` (created per `labs/SETUP.md`).

```bash
# Start full stack including Kafka Connect, PostgreSQL, and MinIO
docker compose --profile connect up -d

# Verify all services are healthy
docker compose ps
curl http://localhost:8083/connectors           # Kafka Connect REST API
curl http://localhost:9000/minio/health/live    # MinIO health check
```

> **First boot takes a few minutes.** On the very first `up`, the `kafka-connect`
> container downloads the JDBC and S3 connector plugins from Confluent Hub before
> the REST API on `:8083` comes up — so the `curl http://localhost:8083/connectors`
> above will fail with "connection refused" until it finishes. Wait until it
> returns `[]`, then continue:
>
> ```bash
> until curl -sf http://localhost:8083/connector-plugins >/dev/null; do sleep 5; done
> echo "Kafka Connect is ready."
> ```
>
> This lab requires `jq` and `curl` on the host (see Prerequisites). On macOS:
> `brew install jq`.

---

## Exercise 1 — Prepare Source Data (PostgreSQL)

> **What this shows:** This builds the upstream system of record the JDBC source connector will poll. The `updated_at` timestamp column and the monotonic `id` are not incidental — they are exactly what the connector uses to track "what have I already read." Seeding 500 rows gives us a known baseline so later exercises can prove that inserts, pauses, and resumes are captured with no gaps or duplicates.
>
> **If a sharp student asks:** Kafka Connect is not true log-based CDC here — a JDBC poll connector only sees committed rows via `SELECT ... WHERE`, so it cannot capture `DELETE`s and can miss updates that don't bump `updated_at`. For real CDC (deletes, before/after images) you'd use a log-reading connector like Debezium that tails the Postgres WAL.

### 1.1 Create the orders table and seed data

```bash
docker exec -it postgres psql -U kafka_user -d orders_db
```

```sql
CREATE TABLE orders (
    id           SERIAL PRIMARY KEY,
    order_id     VARCHAR(50) UNIQUE NOT NULL,
    customer_id  VARCHAR(50) NOT NULL,
    amount       DECIMAL(10,2) NOT NULL,
    status       VARCHAR(20) DEFAULT 'PENDING',
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_updated_at ON orders(updated_at);

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

> **What this shows:** Deploying a connector is just a POST of JSON config to the Connect REST API — no code, no restart. `mode: timestamp+incrementing` is the robust choice: the timestamp column detects updates while the incrementing `id` disambiguates rows sharing the same timestamp, so nothing is lost when many rows land in the same second. Expect the first poll to bulk-load all 500 seeded rows into `prod.postgres.orders`, then near-real-time capture of new inserts every `poll.interval.ms` (2s). The `InsertField` SMT stamps `source_table` onto every record to show single-message transforms in action.
>
> **If a sharp student asks:** Where is the read position kept? For a source connector it's the Connect worker's internal `connect-offsets` topic (a compacted topic), keyed by the connector's source-partition — here the last-seen `updated_at`/`id` pair — not in Postgres and not in the data topic. That's why the position survives worker restarts, and why deleting `connect-offsets` (or renaming the connector) makes it re-read from the beginning.

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
      "connection.password": "kafka_pw",
      "table.whitelist": "orders",
      "mode": "timestamp+incrementing",
      "timestamp.column.name": "updated_at",
      "incrementing.column.name": "id",
      "numeric.mapping": "best_fit",
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

> **Why:** `tasks.max: 1` is not conservatism — a JDBC source in incrementing/timestamp mode cannot split a single table across tasks, because each task would race the same `id`/`updated_at` cursor. One task per table is the hard ceiling here; you scale by giving the connector more tables, not more tasks (revisited in Exercise 6).

### 2.2 Verify connector status

```bash
curl http://localhost:8083/connectors/postgres-orders-source/status | jq .
curl http://localhost:8083/connectors/postgres-orders-source/tasks | jq .
```

### 2.3 Verify events flowing into Kafka

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
docker exec postgres psql -U kafka_user -d orders_db -c "
INSERT INTO orders (order_id, customer_id, amount, status)
SELECT 'new-order-' || i, 'customer-' || (i % 50), random() * 500, 'PENDING'
FROM generate_series(501, 550) AS t(i);"

docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prod.postgres.orders \
  --group lab4-observer \
  --timeout-ms 10000
```

**Questions:**

1. How many events were read on the first poll?
2. How quickly did new inserts appear in Kafka?
3. What fields are present in each event?

---

## Exercise 3 — Deploy S3 Sink Connector (MinIO)

> **What this shows:** A sink connector consumes the Kafka topic as an ordinary consumer group and lands records into object storage — here MinIO speaking the S3 API. The interesting part is *file boundaries*: `flush.size` (record count) and `rotate.interval.ms` (wall-clock) decide when a file is closed, while the `TimeBasedPartitioner` with `timestamp.extractor: RecordField` reads `updated_at` from each record to build `year=/month=/day=/hour=` paths — the Hive-style layout that makes the output queryable by Athena/Spark. Expect JSON objects to appear under time-partitioned prefixes within ~30s.
>
> **If a sharp student asks:** Does a sink connector guarantee exactly-once? No — the S3 sink is effectively at-least-once and achieves *idempotent* output through deterministic file naming: the object name encodes the Kafka offset of the first record, so a replay after failure overwrites the same object rather than appending a duplicate. Connect commits consumer offsets only after a successful upload, so on crash it reprocesses from the last committed offset.

### 3.1 Create MinIO bucket

```bash
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
# The connect profile's minio-setup service already provisioned the bucket.
# Create it here only if it does not exist (safe to re-run):
docker exec minio mc mb --ignore-existing local/kafka-data-lake
docker exec minio mc ls local/
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
      "partition.duration.ms": "3600000",
      "path.format": "'\''year'\''=YYYY/'\''month'\''=MM/'\''day'\''=dd/'\''hour'\''=HH",
      "locale": "en_US",
      "timezone": "UTC",
      "timestamp.extractor": "RecordField",
      "timestamp.field": "updated_at",
      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin"
    }
  }' | jq .
```

> **Why:** `timestamp.extractor: RecordField` + `timestamp.field: updated_at` partitions by the *event's* business time, not the moment Connect happened to write it (`Wallclock`). This is what makes late-arriving or replayed data land in the correct hour partition — critical for correctness in a data lake, and a classic student "gotcha" when files show up under the wrong hour.

### 3.3 Verify files in MinIO

```bash
sleep 35
docker exec minio mc ls --recursive local/kafka-data-lake/
```

**Questions:**

1. What is the time-based path structure of the written files?
2. What determines when a new file is created (`flush.size` vs `rotate.interval.ms`)?
3. How many records are in each file?

---

## Exercise 4 — Error Handling and Dead Letter Queue

> **What this shows:** By default a single poison record halts the task — the connector goes `FAILED` and the whole pipeline stops. This exercise flips on Connect's error-handling framework so bad records are *tolerated*, logged, and routed to a `orders-dlq` topic instead of killing the task. The DLQ is a real Kafka topic, so it's durable and re-consumable. Expect the four malformed messages to land in `orders-dlq` while good records keep flowing to S3.
>
> **If a sharp student asks:** A DLQ only catches failures in the *convert + transform* stage (deserialization, SMT errors) — it does **not** catch failures inside the sink's `put()` (e.g. S3/MinIO being unreachable). Those are retried per `errors.retry.timeout` and then fail the task. So the DLQ is for bad *data*, not bad *infrastructure*, and that distinction is exactly what advanced students probe.

### 4.1 Update the sink connector to add DLQ configuration

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
    "behavior.on.null.values": "ignore",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "errors.deadletterqueue.topic.name": "orders-dlq",
    "errors.deadletterqueue.topic.replication.factor": "3",
    "errors.deadletterqueue.context.headers.enable": "true",
    "aws.access.key.id": "minioadmin",
    "aws.secret.access.key": "minioadmin"
  }' | jq .
```

> **Why:** The DLQ only activates with `errors.tolerance: all` **and** a named `errors.deadletterqueue.topic.name` — set one without the other and records are either dropped silently or the DLQ is never created. Note also this is a PUT to `.../config` (full-config replace, upsert semantics), not a POST, so every key must be present or it's dropped from the connector.

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
    print(f'Injected bad record {i}: {record[:50]}')

producer.flush()
print('Done.')
```

```bash
python inject_bad_records.py
sleep 10

docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --from-beginning \
  --property print.headers=true \
  --property print.key=true \
  --max-messages 4
```

**Questions:**

1. What error headers are attached to DLQ messages?
2. Without `errors.tolerance=all`, what would happen when a bad record arrives?
3. How would you build a DLQ reprocessing pipeline?

---

## Exercise 5 — Connector Failure and Offset Resume

> **What this shows:** Pausing stops the tasks but keeps the connector and its committed offsets intact — it is not a failure, it's a controlled stop. Rows inserted during the pause accumulate in Postgres; on resume the connector reads its stored cursor from `connect-offsets`, issues the next poll `WHERE updated_at/id > last-seen`, and catches up every missed row with no gaps and no duplicates. This is the exercise that makes offset management tangible.
>
> **If a sharp student asks:** What if you delete and recreate the connector under the same name? It resumes from the *same* stored offset, because source offsets are keyed by the source-partition (the table), not by the connector's lifecycle — the position lives in `connect-offsets`, which outlives the connector. To truly re-read from scratch you must reset that offset (delete/override the offset entry) or use the Connect offsets REST endpoint, not just recreate the connector.

### 5.1 Pause the source connector

```bash
curl -X PUT http://localhost:8083/connectors/postgres-orders-source/pause
curl http://localhost:8083/connectors/postgres-orders-source/status | jq '.connector.state'
```

### 5.2 Insert rows while connector is paused

```bash
docker exec postgres psql -U kafka_user -d orders_db -c "
INSERT INTO orders (order_id, customer_id, amount)
SELECT 'paused-order-' || i, 'customer-' || i, 100
FROM generate_series(1, 100) AS t(i);"

echo '100 rows inserted while connector was paused'
```

### 5.3 Resume and verify catch-up

```bash
curl -X PUT http://localhost:8083/connectors/postgres-orders-source/resume

docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prod.postgres.orders \
  --group lab4-catchup \
  --from-beginning \
  --timeout-ms 15000 \
  --property print.key=true \
  | grep paused-order
```

**Questions:**

1. Did the connector pick up all rows inserted while paused?
2. How did Kafka Connect know exactly where to resume?
3. What would happen if you deleted and recreated the connector from scratch?

---

## Exercise 6 — Task Scaling Discussion

> **What this shows:** Setting `tasks.max: 3` on the JDBC source is a deliberate anticlimax — check `/status` and you'll still see exactly **one** running task. `tasks.max` is only a *ceiling*; the connector decides how many tasks it can actually create, and an incrementing/timestamp JDBC source is capped at one task per table. The lesson: throughput scaling depends on the connector's ability to partition its work, not on the number you ask for.
>
> **If a sharp student asks:** Which connectors scale linearly with `tasks.max`? Ones with independently splittable work — a **sink** connector scales up to the number of topic partitions (each task owns a subset, like any consumer group), and sources with multiple tables/files/shards spread those across tasks. So the real ceiling is `min(tasks.max, number of partitionable work units)`; asking for more tasks than partitions just leaves tasks idle.

```bash
# Insert 5000 rows and measure ingestion time
docker exec postgres psql -U kafka_user -d orders_db -c "
INSERT INTO orders (order_id, customer_id, amount)
SELECT 'bulk-order-' || i, 'customer-' || (i % 1000), random() * 1000
FROM generate_series(1, 5000) AS t(i);"

# Try increasing tasks.max (note: JDBC with incrementing mode supports only 1 task per table)
curl -X PUT http://localhost:8083/connectors/postgres-orders-source/config \
  -H "Content-Type: application/json" \
  -d "$(curl -s http://localhost:8083/connectors/postgres-orders-source/config | jq '. + {"tasks.max": "3"}')" \
  | jq .
```

**Questions:**

1. Did increasing `tasks.max` improve throughput for the JDBC connector?
2. For what types of connectors does `tasks.max` provide linear scaling?
3. What is the difference between connector-level parallelism and topic partition parallelism?

---

## Lab Summary

You deployed and operated:

- A JDBC source connector polling PostgreSQL CDC into Kafka
- An S3/MinIO sink connector writing time-partitioned JSON files
- DLQ error handling — configured, injected bad records, and observed routing
- Connector pause/resume with offset tracking verification
- Task scaling analysis and parallelism limits

**Key takeaway:** Kafka Connect eliminates the need for custom integration code. Understanding offset management, error handling, and task parallelism lets you build reliable, production-grade pipelines.

---

## Review Questions

1. Where does a JDBC source connector store its read position, and what happens if that storage is lost?
2. What happens to events that fail processing when `errors.tolerance=all`?
3. What determines the maximum useful `tasks.max` for a given connector?
4. How would you monitor connector health in production?

---

## What's Next

**Module 5** covers reliability, performance tuning, and cluster scaling — you will stress-test a Kafka cluster and analyze rebalance and failover behavior under load.
