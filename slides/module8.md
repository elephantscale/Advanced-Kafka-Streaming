# Module 8 — Observability & Operations

Elephant Scale

---

## Module 8 Agenda

- Core Kafka metrics: broker, producer, consumer
- Monitoring tools: Prometheus, Grafana, Burrow, Cruise Control
- Alerting and anomaly detection
- Rolling upgrades and restarts
- Troubleshooting production issues

---

## Observability Principles for Kafka

The **four signals** for Kafka observability:

1. **Latency** — end-to-end produce/consume latency
2. **Traffic** — messages/sec, bytes/sec per topic/partition
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

 Metric  JMX Path  Alert Threshold
---------
 Under-replicated partitions  `ReplicaManager/UnderReplicatedPartitions`  > 0
 Active controller count  `KafkaController/ActiveControllerCount`  != 1
 Leader count  `ReplicaManager/LeaderCount`  imbalanced (use Cruise Control)
 Request handler idle %  `KafkaRequestHandlerPool/RequestHandlerAvgIdlePercent`  < 30%
 Log flush latency  `Log/LogFlushRateAndTimeMs`  p99 > 500ms
 Network processor idle %  `SocketServer/NetworkProcessorAvgIdlePercent`  < 30%
 Bytes in/out per sec  `BrokerTopicMetrics/BytesInPerSec`  > 80% NIC capacity

---

## Critical Producer Metrics

 Metric  Description  Alert When
---------
 `record-error-rate`  Failed send rate  > 0 in production
 `record-retry-rate`  Retry rate  high sustained rate
 `request-latency-avg`  Avg time to broker ack  > SLA (e.g. 500ms)
 `buffer-available-bytes`  Free producer buffer  < 10MB (blocking)
 `batch-size-avg`  Average batch bytes  low = tuning needed
 `compression-rate-avg`  Achieved compression  low = wrong format

---

## Critical Consumer Metrics

 Metric  Description  Alert When
---------
 Consumer lag  Offset lag per partition  > threshold
 `records-consumed-rate`  Throughput  drops unexpectedly
 `fetch-latency-avg`  Time per fetch  high = broker issue
 `commit-latency-avg`  Offset commit time  high = ZK/KRaft issue
 `rebalance-rate-and-time`  Rebalance frequency  frequent = instability
 `assigned-partitions`  Partitions per consumer  imbalanced

---

## Prometheus + JMX Exporter Setup

```yaml
# docker-compose.yml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_JMX_PORT: 9999
      KAFKA_JMX_HOSTNAME: kafka

  jmx-exporter:
    image: bitnami/jmx-exporter:0.20.0
    volumes:
      - ./jmx-config.yml:/opt/jmx/config.yml
    command: ["9404", "/opt/jmx/config.yml"]
    environment:
      JMX_SERVICE_URL: service:jmx:rmi:///jndi/rmi://kafka:9999/jmxrmi
```

```yaml
# prometheus.yml scrape config
scrape_configs:
  - job_name: kafka
    static_configs:
      - targets: ['jmx-exporter:9404']
```

---

## Grafana Dashboards for Kafka

Key dashboards to maintain:

1. **Cluster Overview** — broker count, active controller, under-replicated partitions
2. **Topic Throughput** — bytes in/out per topic, message rate
3. **Consumer Lag** — lag per group per partition (use Burrow for this)
4. **Producer Health** — error rate, retry rate, latency
5. **Disk and Storage** — disk usage per broker, log size per topic
6. **Network** — bytes in/out per broker vs NIC capacity

Confluent provides open-source Grafana dashboards at:
`https://github.com/confluentinc/jmx-monitoring-stacks`

---

## Burrow — Consumer Lag Monitoring

Burrow provides intelligent consumer lag monitoring with **status evaluation**:

```
OK      — consumer is keeping up
WARN    — consumer is falling behind (lag increasing)
ERR     — consumer is stuck (not making progress)
STOP    — consumer has stopped consuming
STALL   — consumer is consuming but not making progress
```

```yaml
# burrow config
[consumer]
[consumer.local_kafka]
class-name="kafka"
cluster=local
servers=["kafka:9092"]
group-blacklist="^(console-consumer|connect-)"

[httpserver]
[httpserver.default]
address=":8000"
```

---

## Cruise Control — Advanced Operations

Cruise Control provides:

1. **Workload monitoring** — resource utilization per broker/partition
2. **Anomaly detection** — detect broker failures, goal violations
3. **Partition rebalancing** — propose and execute partition moves
4. **Self-healing** — auto-remediate detected anomalies

REST API examples:
```bash
# Get cluster load
GET /kafkacruisecontrol/load

# Get rebalance proposal (without executing)
GET /kafkacruisecontrol/rebalance?dryrun=true

# Execute rebalance
POST /kafkacruisecontrol/rebalance
```

---

## Alerting Strategy

Tier 1 — Page immediately:
- Under-replicated partitions > 0 for > 2 minutes
- Active controller count != 1
- Broker down (not restarting)
- Disk > 90% on any broker

Tier 2 — Notify (Slack/email):
- Consumer lag above SLA threshold
- Producer error rate > 0
- Request handler idle < 30%
- ISR shrink events

Tier 3 — Track in dashboard:
- Compression ratios
- Batch sizes
- Rebalance frequency

---

## Rolling Upgrades

Kafka supports **zero-downtime rolling upgrades**:

```
Step 1: Upgrade broker N (restart one at a time)
  - Controller migrates away from broker N
  - Broker N stops → upgrade → restart
  - Wait for ISR to fully recover before next broker

Step 2: Set inter.broker.protocol.version (new version)
Step 3: Set log.message.format.version (new version)
Step 4: Upgrade clients (producers, consumers)
```

Key principle: always upgrade **brokers before clients**.

---

## Troubleshooting — Under-Replicated Partitions

**Symptom:** `UnderReplicatedPartitions > 0`

**Causes:**
1. Broker failure — follower went offline
2. Network partition between brokers
3. Follower lagging due to high load
4. Disk full on follower broker

**Investigation:**
```bash
kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --under-replicated-partitions

# Check broker logs for the lagging follower
grep "ReplicaFetcherThread" /var/log/kafka/server.log
```

---

## Troubleshooting — Consumer Lag Spikes

**Symptom:** Consumer lag suddenly increases

**Investigation checklist:**
1. Check consumer thread count — are all instances running?
2. Check consumer processing time — did a slow 3rd-party call appear?
3. Check producer throughput — did a traffic spike occur?
4. Check for rebalance — did a consumer instance die?
5. Check for GC pause — is the consumer JVM GC-ing heavily?

```bash
# Watch lag in real time
watch -n 5 kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe --group payment-service
```

---

## Troubleshooting — Stuck Offsets

**Symptom:** Consumer is running but offsets don't advance

**Causes:**
1. Consumer is stuck in an infinite retry loop on a bad message
2. Consumer is fetching but not committing offsets
3. Consumer is stuck in `poll()` beyond `max.poll.interval.ms`

**Fix:**
```python
# Skip the bad message (at-least-once with poison pill handling)
try:
    process(record)
except PoisonPillException:
    send_to_dlq(record)
    consumer.commit()  # advance past the bad message
```

---

## Troubleshooting — Disk Saturation

**Symptom:** Broker disk > 90%

**Immediate actions:**
1. Reduce `retention.ms` or `retention.bytes` on large topics
2. Trigger log compaction on compacted topics
3. Add tiered storage for the largest topics

```bash
# Check topic disk usage
kafka-log-dirs.sh \
  --bootstrap-server kafka:9092 \
  --topic-list orders,payments \
  --describe | grep "size"

# Temporarily reduce retention
kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --alter \
  --entity-type topics \
  --entity-name orders \
  --add-config retention.ms=86400000
```

---

## Troubleshooting — Broker Imbalance

**Symptom:** One broker has 3x the traffic of others

**Cause:** Partition leaders are not evenly distributed.

```bash
# Check leader distribution
kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe | grep "Leader:"

# Trigger preferred replica election (rebalances leaders)
kafka-preferred-replica-election.sh \
  --bootstrap-server kafka:9092
```

Or use **Cruise Control** to automatically rebalance.

---

## Module 8 Summary

- Monitor four signals: latency, traffic, errors, saturation
- Critical broker metrics: under-replicated partitions, active controller, request handler idle
- Use Prometheus + JMX Exporter + Grafana for metrics pipeline
- Burrow provides intelligent consumer lag monitoring with status classification
- Cruise Control automates rebalancing and anomaly detection
- Rolling upgrades: one broker at a time, wait for ISR recovery
- Troubleshooting playbooks: URPs, lag spikes, stuck offsets, disk saturation

---

## What's Next

**Module 9 — Modern Kafka & Streaming Trends**

- Multi-cluster federation and DR
- Kafka at the edge and IoT
- AI-driven event processing
- Serverless Kafka

---

## Lab Preview — Lab 8

**Diagnose and Resolve Cluster Degradation Scenarios**

You will:
1. Set up Prometheus + Grafana monitoring for a Kafka cluster
2. Simulate an under-replicated partition scenario (kill a broker)
3. Diagnose a consumer lag spike and apply a fix
4. Simulate disk saturation and apply retention changes
5. Observe Burrow status transitions

Environment: Docker Compose (3-broker Kafka, Prometheus, Grafana, Burrow)
Time: 75 minutes

---

