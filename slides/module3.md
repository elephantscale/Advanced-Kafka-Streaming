# Module 3 — Kafka Operations & Observability

Elephant Scale

---

## Module 3 Agenda

- The four observability signals for Kafka
- Critical broker, producer, and consumer metrics
- Monitoring and tooling stack: Prometheus, Grafana, Kafka UI, Kafdrop, Confluent Control Center
- Operational procedures: topic management, consumer group management, security operations
- Incident triage runbook

---

## The Four Observability Signals

1. **Latency** — end-to-end produce/consume latency
2. **Traffic** — messages/sec, bytes/sec per topic and partition
3. **Errors** — failed produce requests, consumer errors, ISR shrinks
4. **Saturation** — disk usage, network, CPU, consumer lag

> If you can't measure it, you can't operate it.

---

## Kafka Metrics Architecture

```
Kafka Brokers / Clients
    │  (JMX metrics)
    ▼
JMX Exporter (Prometheus)
    │  (HTTP /metrics endpoint)
    ▼
Prometheus
    │  (scrape + store time series)
    ▼
Grafana
    │  (dashboards + alerts)
    ▼
Alertmanager → PagerDuty / Slack / Email
```

---

## Critical Broker Metrics

| Metric | Alert Threshold |
|--------|----------------|
| Under-replicated partitions | > 0 (page immediately) |
| Active controller count | != 1 (page immediately) |
| Request handler idle % | < 30% (warning) |
| Network processor idle % | < 30% (warning) |
| Log flush latency p99 | > 500ms |
| Bytes in/out per sec | > 80% NIC capacity |
| ISR shrink rate | any non-zero sustained rate |

---

## Critical Consumer Metrics

| Metric | Alert When |
|--------|-----------|
| Consumer lag | > SLA threshold |
| `records-consumed-rate` | drops unexpectedly |
| `fetch-latency-avg` | high → broker issue |
| `rebalance-rate-and-time` | frequent → instability |
| `assigned-partitions` | imbalanced across consumers |

> **Kafka 4 — KIP-848 rebalance protocol:** groups on the new `group.protocol=consumer`
> rebalance incrementally (broker-coordinated), so `rebalance-rate-and-time` should be
> far lower than on the old client-side protocol. Check a group's protocol with
> `kafka-consumer-groups.sh --describe --group <g> --state`.

---

## Monitoring & Tooling Stack

| Tool | Best For |
|------|---------|
| **Prometheus + Grafana** | Broker and consumer dashboards, alerting, lag thresholds |
| **Kafka UI** | Topic management, consumer group operations, visual inspection |
| **Kafdrop** | Lightweight quick inspection of topic contents and consumer state |
| **Confluent Control Center** | Commercial option — enterprise observability with schema/connector visibility (not in this course's lab stack) |

> This course's lab environment uses **Prometheus + Grafana + Kafka UI**. Control Center
> is listed for awareness; it is a paid Confluent product and not required here.

---

## Prometheus Alerting Strategy

**Tier 1 — Page immediately:**
- `UnderReplicatedPartitions > 0` for > 2 minutes
- `ActiveControllerCount != 1`
- Broker down (not restarting)
- Disk > 90% on any broker

**Tier 2 — Notify (Slack/email):**
- Consumer lag above SLA threshold
- Producer error rate > 0
- Request handler idle < 30%

**Tier 3 — Track in dashboard:**
- Compression ratios, batch sizes, rebalance frequency

---

## Topic Operational Procedures

**Alter retention on a live topic:**
```bash
kafka-configs.sh --bootstrap-server kafka:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=86400000
```

**Trigger log compaction:**
```bash
kafka-configs.sh --bootstrap-server kafka:9092 \
  --alter --entity-type topics --entity-name user-profiles \
  --add-config min.cleanable.dirty.ratio=0.01
```

**Inspect topic config:**
```bash
kafka-configs.sh --bootstrap-server kafka:9092 \
  --describe --entity-type topics --entity-name orders
```

---

## Consumer Group Operations

**Check lag across all groups:**
```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --all-groups
```

**Reset offsets to earliest:**
```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group payment-service --topic orders \
  --reset-offsets --to-earliest --execute
```

**Detect stuck consumers:**
- Lag grows but `records-consumed-rate` is non-zero → slow processing
- Lag grows and `records-consumed-rate` is zero → consumer stuck or stopped

---

## Security Operations

**TLS certificate rotation:**
- Generate new keystores before expiry
- Rolling restart one broker at a time
- Verify listeners are up before continuing

**SASL credential rotation:**
```bash
kafka-configs.sh --bootstrap-server kafka:9092 \
  --alter --entity-type users --entity-name alice \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=newpassword]'
```

**Audit logging:**
- Raise the `kafka.authorizer.logger` level in the broker logging config
- Note: Kafka 4 moved from Log4j 1.x to **Log4j2** — configure this in `log4j2.yaml`, not the old `log4j.properties` format
- Route authorizer logs to a dedicated topic/SIEM for compliance tracking

---

## Incident Triage Runbook

Apply this 6-step process to every production incident:

1. **Scope** — one topic? one consumer group? entire cluster?
2. **Control plane** — `ActiveControllerCount` must be exactly 1
3. **Durability** — check `UnderReplicatedPartitions` and ISR state
4. **Consumer impact** — is lag growing or stable?
5. **Mitigation** — one safe, reversible action
6. **Recovery** — verify the signal clears

---

## Troubleshooting — Under-Replicated Partitions

**Symptom:** `UnderReplicatedPartitions > 0`

**Causes:** broker failure, network partition, disk full on follower, follower behind on load

**Investigation:**
```bash
kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --under-replicated-partitions

# Check follower broker logs
grep "ReplicaFetcherThread" /var/log/kafka/server.log
```

---

## Troubleshooting — Consumer Lag Spikes

**Investigation checklist:**
1. Are all consumer instances running?
2. Did processing time increase (slow 3rd-party call)?
3. Did producer throughput spike?
4. Did a rebalance occur (consumer instance died)?
5. Is the consumer JVM GC-ing heavily?

```bash
watch -n 5 kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe --group payment-service
```

---

## Troubleshooting — Disk Saturation

**Immediate actions:**
```bash
# Reduce retention on large non-critical topics
kafka-configs.sh --bootstrap-server kafka:9092 \
  --alter --entity-type topics --entity-name clickstream \
  --add-config retention.ms=3600000

# Check which topics are consuming the most disk
kafka-log-dirs.sh --bootstrap-server kafka:9092 \
  --topic-list orders,payments --describe | grep "size"
```

---

## Module 3 Summary

- Monitor four signals: latency, traffic, errors, saturation
- Critical alert: `UnderReplicatedPartitions > 0`, `ActiveControllerCount != 1`
- Use Prometheus + Grafana for metrics; Kafka UI or Kafdrop for operational inspection
- Topic management: alter retention and compaction settings on live topics
- Consumer group management: describe, reset offsets, detect stuck consumers
- Security operations: certificate rotation, SASL credential management, audit logging
- Use a structured 6-step runbook for every production incident

---

## What's Next

**Module 4 — Connectors, Pipelines & Integrations**

- Kafka Connect deep dive: source, sink, offset management, DLQ
- Integration patterns: S3, Elasticsearch, Flink, Spark, Iceberg/lakehouse
- Enterprise integration patterns and backpressure management

---

## Lab Preview — Lab 3

**Diagnose Kafka Health Using Monitoring Dashboards and Operational Runbooks**

You will:
1. Verify Prometheus metric scraping and Grafana dashboards
2. Produce consumer lag and apply the triage runbook
3. Alter topic retention and reset consumer group offsets
4. Simulate under-replicated partitions (broker kill) and verify recovery
5. Test Prometheus alert expressions

Environment: Docker Compose (3-broker Kafka, Prometheus, Grafana, Kafdrop)
Time: 60–75 minutes

---

