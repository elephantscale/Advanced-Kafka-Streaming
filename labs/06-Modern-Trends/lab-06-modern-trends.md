# Lab 6 — Modern Kafka & Streaming Trends

- **Module:** 6 — Modern Kafka & Streaming Trends
- **Duration:** 60 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

---

## Objectives

By the end of this lab you will be able to:

- Build a lightweight edge-to-core event pipeline with filtering
- Implement a real-time feature enrichment stage
- Integrate a mock inference service with Kafka events
- Demonstrate queue-style work distribution using consumer groups
- Assess a pipeline's serverless readiness using a structured checklist

---

## Prerequisites

- Local Kafka cluster running
- Python 3.9+ with `confluent-kafka` and `flask`

```bash
pip install confluent-kafka flask
```

---

## Lab Environment

> **Lab environment (same across all seven labs):** Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free. These labs use a local **Docker Compose** cluster; the main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`. Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster; if a step reports one unavailable, treat it as instructor-led. Full setup and prerequisites: `labs/SETUP.md`.

```bash
docker compose up -d
docker compose ps
```

---

## Exercise 1 — Edge-to-Core Pipeline Simulation

### 1.1 Create topics

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic edge.telemetry.raw \
  --partitions 3 --replication-factor 3

docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic core.telemetry.filtered \
  --partitions 6 --replication-factor 3
```

### 1.2 Produce simulated edge device events

```python
# edge_producer.py
from confluent_kafka import Producer
import json, random, time

p = Producer({'bootstrap.servers': 'localhost:9092'})

for i in range(5000):
    evt = {
        'device_id': f'd-{i % 200}',
        'temp': round(random.uniform(20, 110), 2),
        'site': random.choice(['factory-a', 'factory-b']),
        'ts': int(time.time() * 1000)
    }
    p.produce('edge.telemetry.raw', key=evt['device_id'], value=json.dumps(evt).encode())
    p.poll(0)

p.flush()
print('Produced 5000 edge events')
```

```bash
python edge_producer.py
```

### 1.3 Filter and forward only critical telemetry (temp >= 80)

```python
# edge_filter_forwarder.py
from confluent_kafka import Consumer, Producer
import json

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'edge-forwarder-cg',
    'auto.offset.reset': 'earliest'
})
p = Producer({'bootstrap.servers': 'localhost:9092'})
c.subscribe(['edge.telemetry.raw'])

forwarded = 0
while forwarded < 1000:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    evt = json.loads(msg.value())
    if evt['temp'] >= 80:
        evt['priority'] = 'high'
        p.produce('core.telemetry.filtered', key=msg.key(), value=json.dumps(evt).encode())
        p.poll(0)
        forwarded += 1

p.flush()
c.close()
print(f'Forwarded {forwarded} high-priority events')
```

**Questions:**
1. Roughly what percentage of events passed the filter?
2. Where is the better place to filter — at the producer, in a pipeline stage, or at the consumer? Why?

---

## Exercise 2 — Streaming Feature Enrichment

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic core.telemetry.features \
  --partitions 6 --replication-factor 3
```

```python
# feature_enricher.py
from confluent_kafka import Consumer, Producer
import json
from collections import defaultdict, deque

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'feature-enricher-cg',
    'auto.offset.reset': 'earliest'
})
p = Producer({'bootstrap.servers': 'localhost:9092'})
c.subscribe(['core.telemetry.filtered'])

windows = defaultdict(lambda: deque(maxlen=20))
processed = 0

while processed < 1000:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    evt = json.loads(msg.value())
    did = evt['device_id']
    windows[did].append(evt['temp'])
    vals = list(windows[did])
    feat = {
        'device_id': did,
        'temp': evt['temp'],
        'moving_avg_20': round(sum(vals) / len(vals), 3),
        'delta_from_avg': round(evt['temp'] - sum(vals) / len(vals), 3),
        'site': evt['site'],
        'ts': evt['ts']
    }
    p.produce('core.telemetry.features', key=msg.key(), value=json.dumps(feat).encode())
    p.poll(0)
    processed += 1

p.flush()
c.close()
print('Enriched 1000 feature events')
```

```bash
python feature_enricher.py
```

**Questions:**
1. Why use a rolling window keyed by `device_id` rather than a global window?
2. What happens to state if this enricher process restarts mid-stream?

---

## Exercise 3 — Mock Inference Integration

### 3.1 Start a mock model scoring service

```python
# mock_model.py
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.post('/score')
def score():
    data = request.json
    risk = 0.1
    if data['temp'] > 95:
        risk = 0.9
    elif data['delta_from_avg'] > 10:
        risk = 0.7
    return jsonify({'risk_score': risk})

app.run(host='0.0.0.0', port=5001)
```

```bash
python mock_model.py &
```

### 3.2 Deploy a streaming inference pipeline

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic core.telemetry.predictions \
  --partitions 6 --replication-factor 3
```

```python
# inference_pipeline.py
from confluent_kafka import Consumer, Producer
import json, requests

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'inference-cg',
    'auto.offset.reset': 'earliest'
})
p = Producer({'bootstrap.servers': 'localhost:9092'})
c.subscribe(['core.telemetry.features'])

n = 0
alerts = 0
while n < 500:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    feat = json.loads(msg.value())
    resp = requests.post('http://localhost:5001/score', json=feat, timeout=2)
    score = resp.json()['risk_score']
    out = {**feat, 'risk_score': score, 'alert': score >= 0.8}
    p.produce('core.telemetry.predictions', key=msg.key(), value=json.dumps(out).encode())
    p.poll(0)
    if out['alert']:
        alerts += 1
    n += 1

p.flush()
c.close()
print(f'Processed {n} events, {alerts} alerts raised')
```

**Questions:**
1. What are the latency trade-offs between embedded inference vs external model serving?
2. What happens to the pipeline if the scoring service is temporarily unavailable?
3. How would you implement model versioning in this pipeline?

---

## Exercise 4 — Queue-Style Worker Pattern

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic core.jobs \
  --partitions 6 --replication-factor 3

# Publish 60 jobs
for i in $(seq 1 60); do
  echo "job-$i" | docker exec -i kafka-1 kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic core.jobs
done
```

Run 3 workers in the same consumer group:

```bash
for w in 1 2 3; do
  docker exec kafka-1 kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic core.jobs \
    --group worker-cg \
    --timeout-ms 7000 \
    --property print.partition=true \
    2>/dev/null | sed "s/^/[worker-$w] /" &
done
wait
```

**Questions:**
1. How are the 60 jobs distributed across 3 workers?
2. Can any worker process the same job twice? Why or why not?
3. What caps the maximum parallelism of the worker pool?

> **This is a consumer-group approximation of a queue** — distribution is by *partition*,
> so with 6 partitions you can't usefully run more than 6 workers, and all jobs on one
> partition go to a single worker. Kafka 4's **Share Groups (KIP-932)** are the *native*
> queue: many share consumers pull from the same partitions with per-message ack, so
> worker count is decoupled from partition count. If the lab cluster has share groups
> enabled, repeat this exercise with `kafka-console-share-consumer.sh --group job-workers`
> and contrast the distribution.

---

## Exercise 5 — Serverless Readiness Checklist

Assess your pipeline against these migration criteria for Amazon MSK Serverless or Confluent Cloud:

| Criteria | Status | Notes |
|---|---|---|
| No broker-specific client assumptions | ✅ / ❌ | |
| Retries and idempotence enabled | ✅ / ❌ | |
| Topic/ACL provisioning scripted | ✅ / ❌ | |
| Metrics exported externally (not broker shell scraping) | ✅ / ❌ | |
| Schema Registry usage standardized | ✅ / ❌ | |
| Cost model understood (traffic/retention pricing) | ✅ / ❌ | |
| No reliance on ZooKeeper or self-managed KRaft config | ✅ / ❌ | |

Document gaps and action items for any ❌ items.

**Questions:**
1. Which of these criteria is hardest to retrofit into an existing pipeline?
2. What is the biggest operational difference between self-managed Kafka and MSK Serverless?
3. When would you choose Confluent Cloud over MSK Serverless?

---

## Lab Summary

You built:

- Edge filtering and forwarding pipeline (raw → filtered)
- Real-time feature enrichment with device-keyed rolling windows
- Event-driven scoring with an external model endpoint
- Queue-like work distribution pattern using consumer groups
- Serverless readiness gap assessment

**Key takeaway:** Modern Kafka architectures are pipeline-centric. Portability, operational contracts, and clear separation of concerns matter more than any single deployment model.

---

## Review Questions

1. Where should filtering happen in an edge pipeline to minimize cost and latency?
2. What is the trade-off between embedded inference and external model serving in a streaming pipeline?
3. Why does consumer group partition assignment cap maximum worker parallelism?
4. What are the top three things to address before migrating a self-managed Kafka cluster to a serverless offering?

---

## What's Next

**Module 7** tackles high-volume fan-out — designing topic layouts and filtering strategies for 10 million messages per second across 10 overlapping consumer groups.

