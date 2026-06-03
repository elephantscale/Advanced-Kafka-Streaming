# Lab 7 — High-Volume Fan-Out Best Practices

- **Module:** 7 — High-Volume Fan-Out Best Practices
- **Duration:** 75–90 minutes
- **Difficulty:** Advanced
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

---

## Scenario

A telemetry stream arrives at **10 million messages per second**. Ten downstream consumer teams each need a **different but overlapping subset** of the data. The goal of this lab is to:

- Design and evaluate topic layout strategies for this scenario
- Implement header-based filtering and measure its CPU savings
- Benchmark duplication (many topics) vs filtering (one topic) at smaller scale
- Configure a KEDA autoscaler to scale consumers automatically based on lag

---

## Objectives

By the end of this lab you will be able to:

- Compare single-topic vs pre-filtered sub-topic designs with real cost reasoning
- Implement and measure header-based consumer-side filtering
- Implement Kafka Streams branching as server-side pre-filtering
- Run a duplication vs filtering benchmark and identify the break-even point
- Configure KEDA to autoscale consumers based on Prometheus consumer lag

---

## Prerequisites

- Running 3-broker Kafka cluster
- Python 3.9+: `pip install confluent-kafka`
- Kubernetes cluster with KEDA installed (optional — see Exercise 5)
- Prometheus scraping Kafka metrics

---

## Lab Environment

> **Lab environment** — same across all seven labs
>
> - Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free.
> - Local **Docker Compose** cluster. The main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
> - Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster. If a step reports one unavailable, treat it as instructor-led.
> - Full setup and prerequisites: `labs/SETUP.md`.

```bash
docker compose up -d
docker compose ps
```

---

## Exercise 1 — Architecture Design: Single Topic vs Sub-Topics

### 1.1 Create the shared broad topic and 3 pre-filtered sub-topics

```bash
# Single broad topic (all data)
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic fanout.telemetry.all \
  --partitions 24 --replication-factor 3

# Pre-filtered sub-topics (server-side split)
for region in emea apac amer; do
  docker exec kafka-1 kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic fanout.telemetry.$region \
    --partitions 8 --replication-factor 3
done
```

### 1.2 Produce events with routing headers

```python
# fanout_producer.py
from confluent_kafka import Producer
import json, random, time

p = Producer({'bootstrap.servers': 'localhost:9092'})

regions = ['emea', 'apac', 'amer']
event_types = ['sensor', 'alert', 'status', 'heartbeat']

for i in range(100000):
    region = random.choice(regions)
    etype = random.choice(event_types)
    evt = {
        'id': i,
        'device_id': f'd-{i % 1000}',
        'region': region,
        'event_type': etype,
        'value': round(random.uniform(0, 100), 2),
        'ts': int(time.time() * 1000)
    }
    p.produce(
        topic='fanout.telemetry.all',
        key=evt['device_id'].encode(),
        value=json.dumps(evt).encode(),
        headers=[
            ('region', region.encode()),
            ('event_type', etype.encode()),
        ]
    )
    p.poll(0)

p.flush()
print('Produced 100,000 events with routing headers')
```

```bash
python fanout_producer.py
```

### 1.3 Design trade-off discussion

Complete this table for the **10M msg/sec, 10 consumer** scenario:

| Design | Storage cost | Network cost | Consumer CPU | Operational complexity |
|---|---|---|---|---|
| 1 broad topic + client-side filter | Low | Low | High (each consumer reads all) | Low |
| 10 pre-filtered topics | 10× storage | 10× producer writes | Low (consumers read only their data) | High (topic sprawl) |
| Streams-branched derived topics | Moderate | Moderate | Low | Moderate |
| Header-based filtering on 1 topic | Low | Low | Moderate (header-only skip) | Low |

**Questions:**
1. At what message rate does topic duplication become cost-prohibitive?
2. What is the cost of deserializing 10M messages/second vs reading headers only?

---

## Exercise 2 — Header-Based Filtering

### 2.1 Consumer that skips by header (no deserialization for unwanted records)

```python
# header_filter_consumer.py
import sys, time, json
from confluent_kafka import Consumer

MY_REGION = sys.argv[1] if len(sys.argv) > 1 else 'emea'

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'header-filter-{MY_REGION}',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['fanout.telemetry.all'])

total = 0
processed = 0
skipped = 0
start = time.time()

try:
    while total < 100000:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        total += 1

        # Header-based skip — no JSON deserialization needed
        headers = dict(msg.headers() or [])
        msg_region = headers.get('region', b'').decode()

        if msg_region != MY_REGION:
            skipped += 1
            continue

        # Only deserialize messages we actually want
        data = json.loads(msg.value())
        processed += 1

        if processed % 1000 == 0:
            elapsed = time.time() - start
            print(f'[{MY_REGION}] processed={processed} skipped={skipped} '
                  f'rate={processed/elapsed:.0f} msg/s')
finally:
    consumer.close()
    elapsed = time.time() - start
    print(f'\n[{MY_REGION}] Final: total={total} processed={processed} '
          f'skipped={skipped} skip_rate={skipped/total*100:.1f}%')
```

```bash
# Run 3 consumers, each filtering for a different region
python header_filter_consumer.py emea &
python header_filter_consumer.py apac &
python header_filter_consumer.py amer &
wait
```

**Questions:**
1. What was the skip rate for each consumer?
2. How much CPU overhead does header-only inspection save vs full deserialization?
3. What is the risk if producers don't consistently set headers?

---

## Exercise 3 — Schema-Based Filtering (Discussion)

Review this approach without running it (requires Avro + Schema Registry):

```python
# Avro union type allows consumers to selectively deserialize
# ProducerRecord with schema:
# {
#   "type": "record",
#   "name": "TelemetryEvent",
#   "fields": [
#     {"name": "region", "type": "string"},
#     {"name": "payload", "type": ["SensorData", "AlertData", "StatusData"]}
#   ]
# }
#
# Consumer deserializes only the outer record first,
# checks region field, then selectively parses payload type.
```

**Discussion questions:**
1. When does schema-based filtering provide more guarantees than header-based filtering?
2. What is the operational cost of maintaining Avro union schemas across 10 consumer variants?
3. How does Schema Registry compatibility enforcement help multi-consumer systems?

---

## Exercise 4 — Benchmark: Duplication vs Filtering

### 4.1 Benchmark full deserialization vs header-only skip

```python
# benchmark_filtering.py
import json, time, random
from confluent_kafka import Producer, Consumer

TOPIC_FULL = 'fanout.telemetry.all'
N = 50000

# --- Producer phase (already produced in Exercise 1) ---

# --- Benchmark A: full deserialization of every record ---
def bench_full_deserialize():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'bench-full',
        'auto.offset.reset': 'earliest',
    })
    c.subscribe([TOPIC_FULL])
    count = 0
    start = time.perf_counter()
    while count < N:
        msg = c.poll(0.5)
        if msg is None or msg.error():
            continue
        _ = json.loads(msg.value())  # full deserialization
        count += 1
    elapsed = time.perf_counter() - start
    c.close()
    return elapsed

# --- Benchmark B: header-only skip ---
def bench_header_skip(target_region='emea'):
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'bench-header',
        'auto.offset.reset': 'earliest',
    })
    c.subscribe([TOPIC_FULL])
    count = 0
    processed = 0
    start = time.perf_counter()
    while count < N:
        msg = c.poll(0.5)
        if msg is None or msg.error():
            continue
        count += 1
        headers = dict(msg.headers() or [])
        if headers.get('region', b'').decode() == target_region:
            _ = json.loads(msg.value())
            processed += 1
    elapsed = time.perf_counter() - start
    c.close()
    return elapsed, processed

t_full = bench_full_deserialize()
t_header, n_proc = bench_header_skip()

print(f'Full deserialization ({N} msgs):   {t_full:.2f}s  ({N/t_full:.0f} msg/s)')
print(f'Header-skip (emea only, {n_proc} msgs): {t_header:.2f}s  ({N/t_header:.0f} msg/s total)')
print(f'Speedup: {t_full/t_header:.2f}×')
```

```bash
python benchmark_filtering.py
```

### 4.2 Record results

| Strategy | Total msgs processed | Elapsed (s) | Throughput (msg/s) |
|---|---|---|---|
| Full deserialization | 50,000 | | |
| Header-based skip | 50,000 | | |
| Speedup | — | — | |

**Questions:**
1. At what fraction of messages being "wanted" does header filtering stop being worthwhile?
2. How does this finding affect your topic design recommendation for 10M msg/sec?

---

## Exercise 5 — KEDA Autoscaler (Kubernetes)

> **Note:** This exercise requires a Kubernetes cluster with KEDA installed. If not available, review the configuration and discuss the behavior.

### 5.1 Deploy a consumer deployment

```yaml
# consumer-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fanout-consumer-emea
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fanout-consumer-emea
  template:
    metadata:
      labels:
        app: fanout-consumer-emea
    spec:
      containers:
      - name: consumer
        image: python:3.11-slim
        command: ["python", "/app/header_filter_consumer.py", "emea"]
```

### 5.2 Create a KEDA ScaledObject

```yaml
# keda-scaledobject.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: fanout-consumer-emea-scaler
spec:
  scaleTargetRef:
    name: fanout-consumer-emea
  minReplicaCount: 1
  maxReplicaCount: 8    # capped by partition count / 3 (24 partitions / 3 regions = 8 max useful)
  pollingInterval: 15
  cooldownPeriod: 60
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      metricName: kafka_consumergroup_lag
      query: |
        max(kafka_consumergroup_lag{consumergroup="header-filter-emea"})
      threshold: "10000"   # scale up when lag exceeds 10,000 records
```

```bash
kubectl apply -f consumer-deployment.yaml
kubectl apply -f keda-scaledobject.yaml

# Watch autoscaler respond to lag
kubectl get hpa -w
```

### 5.3 Simulate load to trigger scale-up

```bash
# Produce a burst of 500,000 events
python fanout_producer.py  # re-run at 5× scale
```

Watch replicas scale from 1 → N and lag decrease as consumers are added.

**Questions:**
1. How does KEDA determine when to scale up vs scale down?
2. What is `cooldownPeriod` protecting against?
3. Why cap `maxReplicaCount` at the number of partitions assigned to this consumer group?

---

## Challenge: Kafka Streams Branching (Optional)

Implement server-side pre-filtering using Kafka Streams, removing filtering CPU from consumers entirely:

```python
# Pseudocode — implement in Java/Scala with the Kafka Streams DSL
# KStream<String, TelemetryEvent> source = builder.stream("fanout.telemetry.all");
#
# Map<String, KStream<String, TelemetryEvent>> branches = source.split()
#     .branch((key, value) -> value.getRegion().equals("emea"), Branched.as("emea"))
#     .branch((key, value) -> value.getRegion().equals("apac"), Branched.as("apac"))
#     .branch((key, value) -> value.getRegion().equals("amer"), Branched.as("amer"))
#     .defaultBranch(Branched.as("other"));
#
# branches.get("emea").to("fanout.telemetry.emea");
# branches.get("apac").to("fanout.telemetry.apac");
# branches.get("amer").to("fanout.telemetry.amer");
```

**Questions:**
1. What are the operational costs of maintaining a Streams branching topology?
2. When does server-side branching become cheaper than client-side filtering?
3. How would you handle schema evolution in a Streams branching pipeline?

---

## Lab Summary

You completed:

- Topic layout design for a high-volume, multi-consumer fan-out scenario
- Header-based filtering implementation with deserialization skip
- Duplication vs filtering benchmark with break-even analysis
- KEDA autoscaler configuration for lag-based consumer scaling
- Review of schema-based and Kafka Streams branching approaches

**Key takeaway:** At high message rates, the right fan-out strategy depends on the ratio of "wanted" to "unwanted" messages, the cost of deserialization, and the operational complexity teams can sustain. Header-based filtering often offers the best balance — low storage overhead, no topic proliferation, and measurable CPU savings.

---

## Review Questions

1. What is the break-even point where topic duplication becomes cheaper than consumer-side filtering?
2. What guarantees do headers provide compared to schema fields? When might headers be unreliable?
3. How does partition count limit the maximum useful number of consumers in a fan-out group?
4. Why is `cooldownPeriod` important in a KEDA autoscaler for Kafka consumers?

---

## What's Next

This is the final lab. Return to the **Module 7 review** to discuss your fan-out strategy choices, trade-offs, and how they apply to your organization's streaming workloads.

