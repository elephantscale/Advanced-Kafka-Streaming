# Lab 10 — Capstone: End-to-End Streaming Architecture

**Module:** 10 — Capstone & Best Practices  
**Duration:** 90 minutes  
**Difficulty:** Advanced

---

## Capstone Scenario

You are designing and validating a production-ready Kafka platform for a global commerce workload.

Requirements:

- Ingest order, payment, inventory, and web events
- Perform near-real-time fraud scoring (< 300 ms target)
- Persist curated data to S3 for analytics
- Provide secure multi-team access with least privilege
- Deliver operational dashboards and incident runbooks

---

## Objectives

By the end of this capstone, you will:

- Design topic taxonomy, partitions, and retention policy
- Implement producer/consumer pipeline pieces
- Add one stream-processing stage and one sink integration
- Apply access controls for two service principals
- Validate reliability via a failover drill
- Produce a short architecture decision record

---

## Deliverables

1. Architecture diagram (ASCII or image)
2. Topic plan table
3. Working proof-of-concept scripts
4. Security and observability checklist completion
5. 1-page ADR (`adr-capstone.md`)

---

## Exercise 1 — Topic and Schema Plan

Create a topic plan in markdown.

| Topic | Key | Partitions | RF | Retention | Policy |
|------|-----|------------|----|-----------|--------|
| `prod.orders.placed` | `order_id` | 12 | 3 | 7d | delete |
| `prod.payments.completed` | `payment_id` | 12 | 3 | 14d | delete |
| `prod.users.profile` | `user_id` | 6 | 3 | long | compact |
| `prod.orders.enriched` | `order_id` | 12 | 3 | 30d | delete |
| `prod.fraud.scores` | `order_id` | 12 | 3 | 30d | delete |

Create topics:

```bash
for t in \
  prod.orders.placed \
  prod.payments.completed \
  prod.users.profile \
  prod.orders.enriched \
  prod.fraud.scores; do
  docker exec kafka-1 kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic "$t" --partitions 12 --replication-factor 3
done

docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name prod.users.profile \
  --add-config cleanup.policy=compact
```

---

## Exercise 2 — Build Minimal Ingest Pipeline

Producer script:

```python
# capstone_producer.py
from confluent_kafka import Producer
import json, random, time

p = Producer({'bootstrap.servers': 'localhost:9092', 'acks': 'all', 'enable.idempotence': True})

for i in range(5000):
    order = {
        'order_id': f'ord-{i}',
        'user_id': f'u-{i % 500}',
        'amount': round(random.uniform(10, 2500), 2),
        'country': random.choice(['US', 'DE', 'IN', 'CA']),
        'ts': int(time.time() * 1000)
    }
    p.produce('prod.orders.placed', key=order['order_id'], value=json.dumps(order).encode())

    payment = {
        'payment_id': f'pay-{i}',
        'order_id': order['order_id'],
        'status': random.choice(['approved', 'approved', 'approved', 'declined']),
        'ts': order['ts']
    }
    p.produce('prod.payments.completed', key=payment['payment_id'], value=json.dumps(payment).encode())

    if i % 1000 == 0:
        p.flush()

p.flush()
print('Produced capstone events')
```

Run:

```bash
python capstone_producer.py
```

---

## Exercise 3 — Enrichment + Fraud Scoring Stage

Create a lightweight enrichment/score consumer:

```python
# capstone_scoring.py
from confluent_kafka import Consumer, Producer
import json

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'capstone-scoring-cg',
    'auto.offset.reset': 'earliest'
})
p = Producer({'bootstrap.servers': 'localhost:9092'})

c.subscribe(['prod.orders.placed'])

processed = 0
while processed < 5000:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    order = json.loads(msg.value())

    # Simple deterministic scoring heuristic
    score = 0.1
    if order['amount'] > 1500:
        score += 0.5
    if order['country'] not in ['US', 'CA', 'DE']:
        score += 0.3

    enriched = {
        **order,
        'risk_score': round(min(score, 0.99), 2),
        'risk_band': 'high' if score >= 0.7 else 'normal'
    }

    p.produce('prod.orders.enriched', key=order['order_id'], value=json.dumps(enriched).encode())
    p.produce('prod.fraud.scores', key=order['order_id'], value=json.dumps({'order_id': order['order_id'], 'risk_score': enriched['risk_score']}).encode())
    p.poll(0)
    processed += 1

p.flush()
c.close()
print('Scored 5000 orders')
```

---

## Exercise 4 — Security Controls (Minimum)

Create service users and ACLs for:
- `svc_capstone_ingest` (write only to ingest topics)
- `svc_capstone_score` (read ingest, write enriched/scores)

Use the same ACL patterns from Lab 7. Validate unauthorized access attempts fail.

---

## Exercise 5 — Reliability Drill

Run a mini-failover test while scoring is active:

```bash
# Start scoring first
python capstone_scoring.py &
SCORE_PID=$!

# Fail one broker
docker compose stop kafka-3
sleep 20

# Recover
docker compose start kafka-3
sleep 30

kill $SCORE_PID
```

Capture:
- URP peak value
- Recovery time
- Any observed produce/consume errors

---

## Exercise 6 — Observability Evidence

Collect and save these outputs in your capstone notes:

```bash
# Topic health
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic prod.orders.placed

# Consumer lag
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group capstone-scoring-cg

# Throughput sample
docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic prod.fraud.scores
```

---

## Exercise 7 — ADR (Architecture Decision Record)

Create `adr-capstone.md` with:

- Context and constraints
- Decision summary
- Why chosen partition counts and keys
- Security model (authn/authz)
- Observability baseline
- Tradeoffs and next improvements

Template:

```markdown
# ADR: Capstone Kafka Architecture

## Context
...

## Decision
...

## Consequences
...

## Next Iteration
...
```

---

## Evaluation Checklist

- [ ] Topic design aligns with access and scaling needs
- [ ] Producer uses idempotence and durable acks where needed
- [ ] Scoring stage produces deterministic outputs
- [ ] ACLs enforce least privilege
- [ ] Failover test completed with recovery evidence
- [ ] Observability evidence captured
- [ ] ADR completed

---

## Lab Summary

You delivered a complete mini-platform spanning ingest, processing, security, reliability, and operations.

**Key takeaway:** Production readiness comes from coherent decisions across architecture, controls, and operations, not from any single Kafka feature.

---

## Review Questions

1. Which design decision most impacted scaling limits?
2. Which control best reduced blast radius in the capstone?
3. What metric would you promote to pager alert first, and why?

---

## Course Complete

You now have a practical foundation to design, deploy, and operate enterprise-grade Kafka streaming platforms.

