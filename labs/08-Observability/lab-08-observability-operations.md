# Lab 8 — Observability and Operations for Kafka

**Module:** 8 — Observability & Operations  
**Duration:** 60-75 minutes  
**Difficulty:** Intermediate

---

## Objectives

By the end of this lab, you will be able to:

- Collect Kafka metrics with Prometheus + JMX exporter
- Visualize broker and consumer health in Grafana
- Track consumer lag and diagnose lag spikes
- Simulate common incidents (URP, lag, disk pressure)
- Use a simple runbook to triage production issues

---

## Prerequisites

- Running Kafka cluster
- Docker Compose profile including Prometheus and Grafana
- CLI access to Kafka tools

---

## Lab Environment

Start monitoring stack:

```bash
docker compose --profile monitoring up -d
docker compose ps
```

Access:
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

---

## Exercise 1 — Verify Metric Scraping

Check Prometheus targets:

```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health, endpoint: .scrapeUrl}'
```

Confirm key broker metrics exist:

```bash
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions' | jq .
curl -s 'http://localhost:9090/api/v1/query?query=kafka_controller_kafkacontroller_activecontrollercount' | jq .
```

Expected:
- `activecontrollercount` should be `1`
- `underreplicatedpartitions` should be `0`

---

## Exercise 2 — Consumer Lag Dashboard

Create a test topic and lagging consumer.

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic obs.lag.demo \
  --partitions 6 --replication-factor 3
```

Produce load:

```bash
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic obs.lag.demo \
  --num-records 100000 \
  --record-size 256 \
  --throughput 15000 \
  --producer-props bootstrap.servers=localhost:9092 acks=1
```

Start one slow consumer and watch lag:

```bash
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group obs-lag-cg
```

Prometheus lag query example (if exporter provides lag metrics):

```bash
curl -s 'http://localhost:9090/api/v1/query?query=max(kafka_consumergroup_lag) by (consumergroup)' | jq .
```

---

## Exercise 3 — Simulate Under-Replicated Partitions

Baseline URP value:

```bash
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions' | jq .
```

Stop one broker:

```bash
docker compose stop kafka-2
sleep 15
```

Check URP and topic state:

```bash
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_underreplicatedpartitions' | jq .

docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions
```

Recover broker:

```bash
docker compose start kafka-2
sleep 30
```

Verify URP returns to zero.

---

## Exercise 4 — Incident Triage Drill

Use this mini-runbook for each incident:

1. Confirm scope (`one topic` vs `whole cluster`)
2. Check control-plane health (`ActiveControllerCount`)
3. Check durability (`UnderReplicatedPartitions`, ISR)
4. Check consumer impact (lag growth trend)
5. Apply one safe mitigation
6. Verify recovery

### Drill A — Consumer Lag Spike

- Symptom: lag grows for `obs-lag-cg`
- Mitigation: add consumer instances or increase partitions

### Drill B — Broker Imbalance

- Symptom: one broker handles most leaders
- Mitigation: preferred leader election or rebalance tooling

### Drill C — Disk Pressure (simulated)

- Symptom: high disk usage trend
- Mitigation: temporary retention reduction on non-critical topics

```bash
# Example retention reduction during incident
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name obs.lag.demo \
  --add-config retention.ms=3600000
```

---

## Exercise 5 — Alert Validation

Create simple alert expressions (Prometheus UI):

- `kafka_server_replicamanager_underreplicatedpartitions > 0`
- `max(kafka_consumergroup_lag) by (consumergroup) > 50000`
- `kafka_controller_kafkacontroller_activecontrollercount != 1`

Test one alert by repeating broker stop/start and observe state transitions.

---

## Lab Summary

You completed:

- Metric ingestion verification
- Lag diagnosis workflow
- URP failover simulation and recovery validation
- Practical runbook-based incident triage

**Key takeaway:** Good operations is fast pattern recognition with safe mitigations, not just dashboards.

---

## Review Questions

1. Why is `UnderReplicatedPartitions` a page-worthy signal?
2. What distinguishes lag caused by producer spikes vs consumer regressions?
3. Which incidents justify immediate retention changes?

---

## What's Next

**Module 9** explores modern Kafka trends: edge, AI integration, queue semantics, and serverless deployments.

