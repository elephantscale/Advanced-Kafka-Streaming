# Lab 3 — Kafka Operations & Observability

**Module:** 3 — Kafka Operations & Observability
**Duration:** 60–75 minutes
**Difficulty:** Intermediate
**Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

---

## Objectives

By the end of this lab you will be able to:

- Collect Kafka metrics with Prometheus + JMX exporter
- Visualize broker and consumer health in Grafana
- Track consumer lag and diagnose lag spikes
- Perform core operational procedures: topic management, consumer group reset, retention change
- Simulate common incidents (under-replicated partitions, lag spike, disk pressure)
- Apply a structured runbook to triage production issues

---

## Prerequisites

- Running 3-broker Kafka cluster (from Lab 2)
- Docker Compose profile including Prometheus and Grafana
- CLI access to Kafka tools

---

## Lab Environment

> **Lab environment (same across all seven labs):** Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free. These labs use a local **Docker Compose** cluster; the main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`. Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster; if a step reports one unavailable, treat it as instructor-led. Full setup and prerequisites: `labs/SETUP.md`.

Start monitoring stack:

```bash
docker compose --profile monitoring up -d
docker compose ps
```

Access:
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Kafdrop: `http://localhost:9000`

---

## Exercise 1 — Verify Metric Scraping

### 1.1 Check Prometheus scrape targets

```bash
curl -s http://localhost:9090/api/v1/targets \
  | jq '.data.activeTargets[] | {job: .labels.job, health: .health, endpoint: .scrapeUrl}'
```

All Kafka broker targets should show `"health": "up"`.

### 1.2 Confirm key broker metrics exist

```bash
# Active controller — should be exactly 1
curl -s 'http://localhost:9090/api/v1/query?query=kafka_controller_kafkacontroller_activecontrollercount' | jq .

# Under-replicated partitions — should be 0 at healthy rest
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions' | jq .
```

**Questions:**
1. How many broker targets are scraping successfully?
2. What does `activecontrollercount != 1` indicate?

---

## Exercise 2 — Consumer Lag Monitoring

### 2.1 Create a test topic and produce load

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic obs.lag.demo \
  --partitions 6 --replication-factor 3

docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic obs.lag.demo \
  --num-records 100000 \
  --record-size 256 \
  --throughput 15000 \
  --producer-props bootstrap.servers=localhost:9092 acks=1
```

### 2.2 Start a slow consumer

```python
# slow_consumer.py
from confluent_kafka import Consumer
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'obs-lag-cg',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
})
consumer.subscribe(['obs.lag.demo'])

count = 0
try:
    while True:
        msg = consumer.poll(0.1)
        if msg and not msg.error():
            time.sleep(0.05)  # simulate processing delay
            count += 1
            if count % 500 == 0:
                print(f'  Processed {count} records')
except KeyboardInterrupt:
    consumer.close()
```

```bash
python slow_consumer.py &
```

### 2.3 Watch lag grow

```bash
watch -n 5 "docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group obs-lag-cg 2>/dev/null"
```

### 2.4 Query lag via Prometheus

```bash
curl -s 'http://localhost:9090/api/v1/query?query=max(kafka_consumergroup_lag) by (consumergroup)' | jq .
```

**Questions:**
1. At what rate is lag growing (records/second)?
2. Which partition shows the highest lag?

---

## Exercise 3 — Topic Operational Procedures

### 3.1 Alter retention on a live topic

```bash
# Reduce retention to 1 hour
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name obs.lag.demo \
  --add-config retention.ms=3600000

# Verify
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --entity-type topics \
  --entity-name obs.lag.demo
```

### 3.2 Reset consumer group offset

```bash
# Stop the consumer first, then reset
kill %1

# Reset to earliest
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group obs-lag-cg \
  --topic obs.lag.demo \
  --reset-offsets \
  --to-earliest \
  --execute

# Verify
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group obs-lag-cg
```

### 3.3 List and inspect all consumer groups

```bash
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list

docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --all-groups 2>/dev/null | head -40
```

**Questions:**
1. What happens to lag after the offset reset?
2. What is the risk of resetting offsets to earliest on a production topic?

---

## Exercise 4 — Simulate Under-Replicated Partitions

### 4.1 Baseline URP value

```bash
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions' | jq .
```

Expected: `0`

### 4.2 Stop one broker and observe URP

```bash
docker compose stop kafka-2
sleep 15

# Check URP via Prometheus
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions' | jq .

# Check via CLI
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions
```

### 4.3 Recover and verify

```bash
docker compose start kafka-2
sleep 30

# Confirm URP returns to zero
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions' | jq .
```

**Questions:**
1. How many partitions became under-replicated?
2. How long did it take for URP to return to zero after restart?
3. What would a Prometheus alert rule look like for this signal?

---

## Exercise 5 — Incident Triage Runbook

Apply this structured 6-step runbook to each simulated incident below:

1. **Scope** — is the issue one topic, one consumer group, or the whole cluster?
2. **Control plane** — check `ActiveControllerCount` (must be 1)
3. **Durability** — check `UnderReplicatedPartitions` and ISR state
4. **Consumer impact** — is lag growing or stable?
5. **Mitigation** — apply one safe, reversible action
6. **Recovery** — verify the signal clears

### Drill A — Consumer Lag Spike

- Symptom: lag growing for `obs-lag-cg`
- Mitigation: add consumer instances to the group

```bash
for i in 1 2; do
  docker exec kafka-1 kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic obs.lag.demo \
    --group obs-lag-cg \
    --timeout-ms 15000 > /dev/null &
done
wait
```

### Drill B — Broker Imbalance

- Symptom: one broker holds most leaders
- Check:

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic obs.lag.demo \
  | awk '/Leader:/ {print $4}' | sort | uniq -c | sort -rn
```

- Mitigation: preferred leader election

```bash
# kafka-preferred-replica-election.sh was removed in Kafka 4 — use kafka-leader-election.sh.
docker exec kafka-1 kafka-leader-election.sh \
  --bootstrap-server localhost:9092 \
  --election-type preferred \
  --all-topic-partitions
```

### Drill C — Disk Pressure (simulated)

- Symptom: high disk usage trend in Grafana
- Mitigation: reduce retention on non-critical topics

```bash
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name obs.lag.demo \
  --add-config retention.ms=900000   # 15 minutes
```

---

## Exercise 6 — Alert Expressions

Create and test these Prometheus alert expressions in the Prometheus UI (`http://localhost:9090`):

```promql
# Under-replicated partitions (critical)
kafka_server_replicamanager_underreplicatedpartitions > 0

# High consumer lag (warning)
max(kafka_consumergroup_lag) by (consumergroup) > 50000

# Missing active controller (critical)
kafka_controller_kafkacontroller_activecontrollercount != 1

# Broker offline (critical)
count(kafka_server_brokerstate) < 3
```

Trigger each condition and observe the alert state change.

---

## Lab Summary

You completed:

- Prometheus metric scraping verification and Grafana visualization
- Consumer lag production, monitoring, and runbook response
- Topic retention and consumer group offset management
- Under-replicated partition simulation and recovery
- Structured incident triage drills (lag spike, broker imbalance, disk pressure)
- Prometheus alert expression validation

**Key takeaway:** Good operations is fast pattern recognition with safe, reversible mitigations. Metrics without runbooks are only half the answer.

---

## Review Questions

1. Why is `UnderReplicatedPartitions > 0` considered a page-worthy alert?
2. What distinguishes consumer lag caused by a producer throughput spike versus a consumer regression?
3. When is it safe to reset consumer group offsets, and when is it risky?
4. What are the three most important metrics to watch during a rolling broker restart?

---

## What's Next

**Module 4** goes into connectors and integrations — you will deploy source and sink connectors and observe how Kafka Connect handles errors and offset recovery.

