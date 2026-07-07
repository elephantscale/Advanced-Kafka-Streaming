# Lab 3 — Kafka Operations & Observability

- **Module:** 3 — Kafka Operations & Observability
- **Duration:** 60–75 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

---

## Objectives

By the end of this lab you will be able to:

- Collect Kafka metrics with Prometheus + kafka-exporter
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

> **Lab environment** — same across all seven labs
>
> - Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free.
> - Local **Docker Compose** cluster. The main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
> - Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster. If a step reports one unavailable, treat it as instructor-led.
> - Full setup and prerequisites: `labs/SETUP.md`.
> - **Python exercises:** activate the virtualenv first — `source .venv/bin/activate` (created per `labs/SETUP.md`).

Start monitoring stack:

```bash
docker compose --profile monitoring up -d
docker compose ps
```

Access:

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (login `admin` / `admin`)
- Kafka UI: `http://localhost:8080`

> **Metrics source:** the monitoring stack scrapes **kafka-exporter**, which exposes
> consumer-group lag and topic/partition health (including under-replicated partitions and
> in-sync replicas). Control-plane facts like the active controller come from the cluster
> directly (`kafka-metadata-quorum.sh`), not the exporter. Grafana comes **pre-provisioned**
> with the Prometheus datasource and a **"Kafka Cluster Overview"** dashboard (in the *Kafka*
> folder) — open Grafana and it's already populated, no setup needed.

---

## Exercise 1 — Verify Metric Scraping

### 1.1 Check Prometheus scrape targets

```bash
curl -s http://localhost:9090/api/v1/targets \
  | jq '.data.activeTargets[] | {job: .labels.job, health: .health, endpoint: .scrapeUrl}'
```

All Kafka broker targets should show `"health": "up"`.

### 1.2 Confirm key broker metrics exist

The monitoring stack scrapes **kafka-exporter**, which exposes topic/partition and
consumer-group metrics. Two of the most important operational signals:

```bash
# Under-replicated partitions — should be 0 at healthy rest
curl -s 'http://localhost:9090/api/v1/query?query=sum(kafka_topic_partition_under_replicated_partition)' | jq '.data.result'

# Number of brokers the exporter can see
curl -s 'http://localhost:9090/api/v1/query?query=kafka_brokers' | jq '.data.result'
```

The **active controller** is a KRaft control-plane fact, not a kafka-exporter metric —
confirm it directly from the cluster (exactly one node is the quorum leader):

```bash
docker exec kafka-1 kafka-metadata-quorum.sh --bootstrap-server localhost:9092 \
  describe --status | grep -E "LeaderId|LeaderEpoch"
```

**Questions:**

1. How many broker targets are scraping successfully?
2. What does `activecontrollercount != 1` indicate?

---

## Exercise 2 — Consumer Lag Monitoring

> **Terminals for this exercise** — this exercise runs things concurrently, so keep track of
> which terminal is which:
>
> | Terminal | Runs | Notes |
> |----------|------|-------|
> | **T1** | 2.1 produce, then 2.2 slow consumer (`… &`) | one-shot commands; the consumer runs in the background here |
> | **T2** | 2.3 `watch` lag | dedicated — `watch` takes over the screen; `Ctrl+C` to exit |
> | **T3** *(optional)* | re-run 2.1 perf-test to make lag climb live | only if you want to *see lag rise* during class |
>
> The one-off queries (2.4 `curl`, and the Grafana browser tab) can run in **any** terminal.
> **Rule of thumb:** anything that *stays running* (a background consumer, `watch`, a live
> producer) gets its **own terminal**; one-shot commands can share T1.

### 2.1 Create a test topic and produce load

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic obs.lag.demo \
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
# In T1. Runs in the background (&) so it keeps consuming slowly while you observe lag.
# Output goes to a log file so it doesn't spam this terminal (tail it any time with
# `tail -f /tmp/cons.log`). You'll stop it in Exercise 3.2 with: pkill -INT -f slow_consumer.py
source .venv/bin/activate
python slow_consumer.py > /tmp/cons.log 2>&1 &
```

### 2.3 Watch lag grow

> **Run this in a separate terminal.** `watch` takes over the screen and refreshes every
> 5s, so it needs its own terminal — the slow consumer from 2.2 keeps running in the first
> one. Leave this open while you observe, then `Ctrl+C` to exit before Exercise 3.

```bash
watch -n 5 "docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group obs-lag-cg 2>/dev/null"
```

The perf-test in 2.1 produced 100k records in a burst and finished; the slow consumer
drains at only ~20 records/sec. So the **LAG** column starts high and ticks down slowly —
that is the lesson: a slow consumer can't keep up, and lag is the early-warning signal. To
watch lag *climb* live, re-run the 2.1 perf-test in a third terminal while the consumer runs.

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
# Stop the slow consumer first — offset reset requires the group to be INACTIVE
# (Kafka refuses to reset a live group). pkill works from ANY terminal; the old
# 'kill %1' only works if the consumer is a background job in this same shell.
# -INT sends Ctrl+C so the script's handler closes the consumer and leaves the group cleanly.
pkill -INT -f slow_consumer.py
sleep 5   # give the group a moment to become empty

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
curl -s 'http://localhost:9090/api/v1/query?query=sum(kafka_topic_partition_under_replicated_partition)' | jq .
```

Expected: `0`

### 4.2 Stop one broker and observe URP

```bash
docker compose stop kafka-2
sleep 15

# Check URP via Prometheus
curl -s 'http://localhost:9090/api/v1/query?query=sum(kafka_topic_partition_under_replicated_partition)' | jq .

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
curl -s 'http://localhost:9090/api/v1/query?query=sum(kafka_topic_partition_under_replicated_partition)' | jq .
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
sum(kafka_topic_partition_under_replicated_partition) > 0

# High consumer lag (warning)
max(kafka_consumergroup_lag) by (consumergroup) > 50000

# Missing active controller (critical)
# active controller: check via kafka-metadata-quorum.sh --describe --status (control-plane, not in kafka-exporter)

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

## Cleanup

Before moving on to Lab 4, tidy up what this lab left running — **no full teardown needed**
(Lab 4 creates its own topics and data; `obs.lag.demo` won't interfere):

```bash
# 1. Stop the slow consumer if it's still running (from Exercise 2)
pkill -f slow_consumer.py

# 2. Make sure all three brokers are up — Exercise 4 stopped kafka-2;
#    confirm 4.3 restarted it (Connect's internal topics are RF=3 and need all three)
docker compose ps
docker compose start kafka-2      # only if kafka-2 shows as stopped

# 3. (Optional) free resources — Lab 4 does not use the monitoring stack
docker compose --profile monitoring stop
```

> Do **not** run `docker compose down` — that wipes the cluster and you would have to
> re-create everything. Just stop the leftover consumer, confirm all brokers are up, and
> (optionally) stop monitoring.

---

## What's Next

**Module 4** goes into connectors and integrations — you will deploy source and sink connectors and observe how Kafka Connect handles errors and offset recovery.

