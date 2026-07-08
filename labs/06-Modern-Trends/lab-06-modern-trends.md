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

> **Lab environment** — same across all seven labs
>
> - Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free.
> - Local **Docker Compose** cluster. The main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
> - Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster. If a step reports one unavailable, treat it as instructor-led.
> - Full setup and prerequisites: `labs/SETUP.md`.
> - **Python exercises:** activate the virtualenv first — `source .venv/bin/activate` (created per `labs/SETUP.md`).

```bash
docker compose up -d
docker compose ps
```

---

## Exercise 1 — Edge-to-Core Pipeline Simulation

> **What this shows:** The classic edge-to-core shape — a high-volume raw topic at the edge, a filter/forward stage, and a smaller curated core topic. The concept that matters is *filtering early*: dropping non-critical events (temp < 80) before they cross the network shrinks bandwidth, storage, and downstream compute, which is the whole economic case for edge processing. Expect roughly a third of the 5000 events to pass (uniform temps over 20–110, so ~30% land at ≥ 80), and the forwarder to stop at exactly 1000 forwarded.
>
> **If a sharp student asks:** Note the deliberate partition bump (raw = 3, filtered = 6) — repartitioning at a stage boundary is legal and common, but it breaks per-key ordering guarantees across the boundary unless you re-key carefully; here the same `device_id` key is preserved, so a given device still lands on one partition of the filtered topic.

### 1.1 Create topics

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic edge.telemetry.raw \
  --partitions 3 --replication-factor 3

docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic core.telemetry.filtered \
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

> **Why:** Run this (and every Python step in the lab) inside the activated virtualenv — without `source .venv/bin/activate`, the `from confluent_kafka import ...` line fails with `ModuleNotFoundError` because the package lives in the venv, not the system interpreter.

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

> **What this shows:** A stateful streaming stage — it turns each raw reading into ML-ready *features* (a 20-sample moving average and the delta from that average) computed per device. The key idea is that streaming feature engineering keeps state in-memory keyed by entity, so features reflect recent history rather than a single point; this is exactly what a downstream model needs to spot anomalies. Expect 1000 enriched events, each carrying `moving_avg_20` and `delta_from_avg`.
>
> **If a sharp student asks:** This in-process `defaultdict` of `deque`s is ephemeral state — if the enricher restarts, every window resets and early moving averages are computed over a partial history until the deques refill. Production systems solve this with fault-tolerant, partitioned state (Kafka Streams `KTable`/RocksDB backed by a changelog topic, or Flink keyed state with checkpoints) so the window survives restarts and rebalances.

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic core.telemetry.features \
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

> **What this shows:** The "AI in the stream" pattern — a Kafka consumer calls an external model service per event and writes scored results back to a topic. The concept is the separation of the *streaming runtime* from the *model runtime*: the model can be deployed, versioned, and scaled independently (a real-world Seldon/KServe/SageMaker endpoint slots in where the Flask mock sits). Expect 500 events scored and a handful flagged as alerts (risk ≥ 0.8, i.e. the temp > 95 branch).
>
> **If a sharp student asks:** Synchronous per-record HTTP calls make the model the throughput bottleneck and a single point of failure — one slow or down endpoint stalls the whole pipeline (here the `timeout=2` will raise and crash the loop rather than degrade gracefully). Production alternatives: micro-batch the requests, run inference async/concurrently, add retries with a circuit breaker, or embed the model in-process to trade operational flexibility for latency.

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

> **Why:** The `&` backgrounds the Flask service so it keeps listening on `:5001` while you run the pipeline in the same shell — the inference pipeline in 3.2 POSTs to `http://localhost:5001/score` on every event, so this must already be up or every request fails with a connection-refused error.

### 3.2 Deploy a streaming inference pipeline

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic core.telemetry.predictions \
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

> **What this shows:** How far a plain consumer group gets you toward queue semantics — three workers sharing `worker-cg` split the 60 jobs so each job is processed once. The concept to land is the ceiling: a consumer group assigns whole *partitions*, so at most one consumer reads a given partition and parallelism is hard-capped at the partition count (6 here); a consumer group is a *log reader that load-balances by partition*, not a true queue with per-message hand-off. Expect the jobs consumed once in total, though the CLI split is usually lopsided (see the note below).
>
> **If a sharp student asks:** This is exactly what Kafka 4's **Share Groups (KIP-932)** fix. In a share group, *many* consumers cooperatively read the *same* partitions with per-record acknowledgement (ack / release / reject), so worker count is decoupled from partition count — you can run 50 workers against 6 partitions, and an un-acked record is redelivered rather than silently owned by an idle consumer. A classic consumer group can't do this because offset commit is per-partition and coarse-grained, which is why it isn't a real queue. Share Groups are early access in Kafka 4 and must be enabled on the cluster — if not, this exercise is the correct consumer-group approximation.

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic core.jobs \
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
    --from-beginning \
    --timeout-ms 7000 \
    --property print.partition=true \
    2>/dev/null | sed "s/^/[worker-$w] /" &
done
wait
```

> **`--from-beginning` matters here.** The 60 jobs were published *before* the
> workers start, and a brand-new consumer group defaults to `auto.offset.reset=latest`
> — so without `--from-beginning` the workers join at the end of the log and consume
> **nothing**. With it, they read all 60.
>
> **Expect uneven splits from the CLI.** Because these are three *separate*
> console-consumer processes racing to join, whichever joins first is assigned the
> partitions and can drain them before the others finish joining — so you may see one
> worker take most or all of the jobs. That is a property of the CLI demo, not of
> consumer groups; the durable teaching point is the one below — distribution is by
> *partition*, so parallelism is capped at the partition count.

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

> **What this shows:** A structured way to judge whether a pipeline is portable to a managed/serverless Kafka (MSK Serverless, Confluent Cloud). The concept is that "cloud-ready" is mostly about *removing broker-host assumptions* — no shelling into brokers for metrics or admin, provisioning as code, idempotent producers, externalized schema — so the workload doesn't care who runs the brokers. Expect this to surface real gaps in the lab pipeline (for example, we scrape metrics via `docker exec`, which a serverless cluster won't allow).
>
> **If a sharp student asks:** The subtlest line is "no reliance on self-managed KRaft config" — serverless offerings hide broker/controller configuration entirely, so anything that tunes `log.*`, sets cluster-level defaults, or assumes a fixed broker count won't port; the fix is to push those concerns into per-topic configs and client settings, which the managed control plane still honors.

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
