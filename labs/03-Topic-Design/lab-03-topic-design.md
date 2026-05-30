# Lab 3 — Topic Design and Schema Management

**Module:** 3 — Advanced Topic Design & Data Modeling
**Duration:** 60 minutes
**Difficulty:** Intermediate

---

## Objectives

By the end of this lab you will be able to:

- Create topics with production-grade configurations
- Identify and fix hot partitions caused by poor key selection
- Register schemas in Schema Registry and enforce compatibility
- Benchmark topic throughput using built-in tools
- Observe schema evolution with compatibility checks
- Configure and verify tiered storage (simulation)

---

## Prerequisites

- Lab 1 and 2 completed
- Docker Compose cluster with Schema Registry added
- Python: `pip install confluent-kafka fastavro`

---

## Lab Environment

```bash
# Start cluster with Schema Registry
docker compose --profile schema-registry up -d

# Verify Schema Registry
curl http://localhost:8081/subjects
```

---

## Exercise 1 — Hot Partition Simulation and Fix

### 1.1 Create a topic with a bad key design

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders-bad-key \
  --partitions 6 --replication-factor 3
```

### 1.2 Produce with a skewed key (country code)

```python
# hot_partition_demo.py
from confluent_kafka import Producer
import json, random

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Simulated traffic: 80% US, 15% EU, 5% APAC
keys_weighted = (
    ['US'] * 800 +
    ['EU'] * 150 +
    ['APAC'] * 50
)

for i in range(1000):
    key = random.choice(keys_weighted)
    event = {'order_id': f'order-{i}', 'region': key, 'amount': random.uniform(10, 500)}
    producer.produce(
        'orders-bad-key',
        key=key,
        value=json.dumps(event).encode()
    )

producer.flush()
print("Produced 1000 events with country key")
```

```bash
python hot_partition_demo.py
```

### 1.3 Check partition distribution

```bash
docker exec kafka-1 kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 \
  --topic-list orders-bad-key \
  --describe \
  | python3 -c "
import sys, json
data = json.loads(sys.stdin.read().split('Querying brokers')[1].split('\n',1)[1] if 'Querying' in sys.stdin.read() else sys.stdin.read())
" 2>/dev/null || \
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group test 2>/dev/null

# Simpler: use kafka-get-offsets.sh
docker exec kafka-1 kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-bad-key
```

**Questions:**
1. Which partition has the most messages?
2. What key maps to that partition?
3. How does this affect consumer performance?

### 1.4 Fix: composite key with user entropy

```python
# fixed_key_producer.py
from confluent_kafka import Producer
import json, random, hashlib

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

docker_exec = '''docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders-good-key \
  --partitions 6 --replication-factor 3'''

import subprocess
subprocess.run(docker_exec, shell=True)

keys_weighted = ['US'] * 800 + ['EU'] * 150 + ['APAC'] * 50

for i in range(1000):
    region = random.choice(keys_weighted)
    user_id = random.randint(1, 10000)
    # Composite key: region + user_id → even distribution
    composite_key = f"{region}_{user_id}"
    event = {'order_id': f'order-{i}', 'region': region, 'user_id': user_id}
    producer.produce(
        'orders-good-key',
        key=composite_key,
        value=json.dumps(event).encode()
    )

producer.flush()
print("Produced 1000 events with composite key")
```

Compare partition distributions between `orders-bad-key` and `orders-good-key`.

---

## Exercise 2 — Schema Registry

### 2.1 Register an Avro schema

```bash
# Register OrderPlaced schema
curl -X POST http://localhost:8081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"com.acme.orders\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"customer_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":{\"type\":\"string\"},\"default\":\"USD\"},{\"name\":\"placed_at\",\"type\":\"long\"}]}"
  }'
```

```bash
# List registered subjects
curl http://localhost:8081/subjects

# Get latest schema for orders-value
curl http://localhost:8081/subjects/orders-value/versions/latest | python3 -m json.tool
```

### 2.2 Produce with Avro serialization

```python
# avro_producer.py
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import time, random

schema_str = """
{
  "type": "record",
  "name": "OrderPlaced",
  "namespace": "com.acme.orders",
  "fields": [
    {"name": "order_id",     "type": "string"},
    {"name": "customer_id",  "type": "string"},
    {"name": "amount",       "type": "double"},
    {"name": "currency",     "type": "string",  "default": "USD"},
    {"name": "placed_at",    "type": "long"}
  ]
}
"""

sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer = Producer({'bootstrap.servers': 'localhost:9092'})

for i in range(10):
    order = {
        'order_id': f'avro-order-{i}',
        'customer_id': f'customer-{random.randint(1,100)}',
        'amount': round(random.uniform(10, 1000), 2),
        'currency': 'USD',
        'placed_at': int(time.time() * 1000)
    }
    producer.produce(
        topic='orders',
        key=order['order_id'],
        value=avro_serializer(order, SerializationContext('orders', MessageField.VALUE))
    )

producer.flush()
print("Produced 10 Avro events")
```

```bash
python avro_producer.py
```

---

## Exercise 3 — Schema Evolution and Compatibility

### 3.1 Add an optional field (BACKWARD compatible)

```bash
# Register version 2 — add optional field
curl -X POST http://localhost:8081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"com.acme.orders\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"customer_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"},{\"name\":\"placed_at\",\"type\":\"long\"},{\"name\":\"promo_code\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }'
```

```bash
# Verify 2 versions now exist
curl http://localhost:8081/subjects/orders-value/versions
```

### 3.2 Try a BACKWARD-incompatible change

```bash
# Try to add a required field (no default) — should FAIL
curl -X POST http://localhost:8081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"com.acme.orders\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"customer_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"placed_at\",\"type\":\"long\"},{\"name\":\"required_new_field\",\"type\":\"string\"}]}"
  }'
```

**Questions:**
1. What error did you get for the incompatible schema?
2. Why is a required field without a default incompatible backward?
3. How would an old consumer reading a new schema handle `promo_code`?

### 3.3 Check compatibility without registering

```bash
curl -X POST http://localhost:8081/compatibility/subjects/orders-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<your-schema-here>"}'
```

---

## Exercise 4 — Throughput Benchmarking

### 4.1 Benchmark producer throughput (baseline)

```bash
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 500000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    acks=1
```

Note the throughput (MB/sec, records/sec).

### 4.2 Benchmark with batching and compression

```bash
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 500000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    acks=1 \
    batch.size=131072 \
    linger.ms=20 \
    compression.type=lz4
```

### 4.3 Benchmark consumer throughput

```bash
docker exec kafka-1 kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic perf-test \
  --messages 500000 \
  --group bench-consumer
```

**Results table:**

| Configuration | Producer MB/s | Consumer MB/s |
|---------------|--------------|--------------|
| Default (acks=1) | | |
| batch.size=128KB + linger.ms=20 + lz4 | | |

**Questions:**
1. How much did batching + compression improve throughput?
2. What is the consumer throughput vs producer throughput?
3. What limits consumer throughput?

---

## Exercise 5 — Topic Naming Convention

### 5.1 Create topics following a naming convention

```bash
for topic in \
  "prod.orders.order.placed" \
  "prod.orders.order.confirmed" \
  "prod.payments.payment.completed" \
  "prod.inventory.stock.updated" \
  "prod.users.user.registered"; do

  docker exec kafka-1 kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic "$topic" \
    --partitions 6 --replication-factor 3
  echo "Created: $topic"
done

# List all production topics
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list | grep "^prod\."
```

**Questions:**
1. What are the benefits of this naming convention?
2. How would you set ACLs based on topic prefix?
3. How would you set retention differently for `order.placed` vs `user.registered`?

---

## Lab Summary

You have practiced:

- Identifying hot partitions caused by low-cardinality keys
- Fixing key design to achieve even partition distribution
- Registering schemas in Schema Registry (Avro)
- Schema evolution: adding optional fields (compatible) vs required fields (incompatible)
- Benchmarking producer and consumer throughput
- Production topic naming conventions

**Key takeaway:** Partition key selection and schema design are decisions that are hard to change later. Get them right before your first production deployment.

---

## Review Questions

1. Why does using `country` as a partition key cause problems for US-heavy traffic?
2. What schema change is guaranteed to be backward-compatible? Give an example.
3. What does `cleanup.policy=compact,delete` mean for a topic?
4. In the benchmark, what configuration change had the biggest impact on throughput?

---

## What's Next

**Module 4** introduces stream processing with Kafka Streams and ksqlDB — you'll build a real-time anomaly detection pipeline.

