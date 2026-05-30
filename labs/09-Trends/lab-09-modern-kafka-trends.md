# Lab 9 — Modern Kafka Trends: Edge, AI, and Serverless

**Module:** 9 — Modern Kafka & Streaming Trends  
**Duration:** 60 minutes  
**Difficulty:** Intermediate

---

## Objectives

By the end of this lab, you will be able to:

- Build a lightweight edge-to-core event pipeline
- Run a simple streaming feature-enrichment stage
- Integrate a mock inference service with Kafka events
- Compare queue-like worker semantics with standard consumer behavior
- Evaluate a minimal serverless Kafka migration checklist

---

## Prerequisites

- Local Kafka cluster running
- Python 3.9+ with `confluent-kafka` and `flask`

```bash
pip install confluent-kafka flask
```

---

## Exercise 1 — Edge-to-Core Pipeline Simulation

Create two topics:

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

Produce edge events:

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

Run:

```bash
python edge_producer.py
```

Filter/forward only critical telemetry (`temp >= 80`):

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

---

## Exercise 2 — Streaming Feature Enrichment

Create a feature topic:

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic core.telemetry.features \
  --partitions 6 --replication-factor 3
```

Feature enrichment app:

```python
# feature_enricher.py
from confluent_kafka import Consumer, Producer
import json, time
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
        'delta_from_avg': round(evt['temp'] - (sum(vals) / len(vals)), 3),
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

---

## Exercise 3 — Mock Inference Integration

Start a mock model service:

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
python mock_model.py
```

Create prediction topic:

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic core.telemetry.predictions \
  --partitions 6 --replication-factor 3
```

Inference pipeline:

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
    n += 1

p.flush()
c.close()
print('Produced 500 prediction events')
```

---

## Exercise 4 — Queue-Style Worker Pattern

Create a tasks topic:

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic core.jobs \
  --partitions 6 --replication-factor 3
```

Publish jobs:

```bash
for i in $(seq 1 60); do
  echo "job-$i" | docker exec -i kafka-1 kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic core.jobs
done
```

Run 3 workers in the same group and observe split processing:

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

---

## Exercise 5 — Serverless Readiness Checklist

Assess your pipeline against this migration checklist:

- No broker-specific client assumptions
- Proper retries and idempotence enabled
- Topic/ACL provisioning scripted
- Metrics exported externally (not broker shell scraping)
- Schema registry usage standardized
- Cost controls (tokenized by traffic/retention)

Document gaps and action items in your notes.

---

## Lab Summary

You built:

- Edge filtering and forwarding
- Real-time feature enrichment
- Event-driven scoring with a model endpoint
- Queue-like worker distribution pattern

**Key takeaway:** Modern Kafka architectures are pipeline-centric. Portability and good operational contracts matter more than any single deployment model.

---

## Review Questions

1. Where should filtering happen in edge pipelines to reduce cost and latency?
2. What is the tradeoff between embedded inference vs external model serving?
3. Why does worker-group partitioning cap parallelism?

---

## What's Next

**Module 10** is the capstone: design, build, and review a production-ready end-to-end streaming architecture.

