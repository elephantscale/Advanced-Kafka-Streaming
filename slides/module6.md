# Module 6 — Reliability, Scaling & Performance

Elephant Scale

---

## Module 6 Agenda

- Throughput tuning parameters
- Detecting and fixing consumer lag
- Cooperative and sticky rebalancing
- Cluster scaling strategies
- Tiered storage optimization
- Disaster recovery and failover patterns
- Capacity planning
- Performance benchmarking methodologies

---

## What Limits Kafka Throughput?

Kafka throughput is limited by:

1. **Network bandwidth** — bytes in/out per broker
2. **Disk I/O** — sequential write throughput
3. **CPU** — compression/decompression, serialization
4. **Producer batching** — small batches = more overhead
5. **Replication factor** — more replicas = more network traffic
6. **Consumer parallelism** — limited by partition count

> In practice: network and disk are the most common bottlenecks.

---

## Producer Throughput Tuning

Key parameters for maximizing producer throughput:

 Parameter  Tuning Direction  Effect
-----------
 `batch.size`  Increase (64KB–1MB)  Larger batches, fewer requests
 `linger.ms`  Increase (5–100ms)  Wait to fill batches, reduces requests
 `compression.type`  `lz4` or `zstd`  Smaller network payload
 `buffer.memory`  Increase (64MB+)  More in-flight data before blocking
 `max.in.flight.requests.per.connection`  5 (default)  Concurrent batches per broker

Example high-throughput config:
```properties
batch.size=131072
linger.ms=20
compression.type=lz4
buffer.memory=67108864
```

---

## Consumer Throughput Tuning

 Parameter  Tuning Direction  Effect
----------
 `fetch.min.bytes`  Increase (1KB–1MB)  Wait for more data per fetch
 `fetch.max.wait.ms`  Increase (500ms)  Max wait time for min bytes
 `max.poll.records`  Increase (500–5000)  More records per poll
 `fetch.max.bytes`  Increase (50MB+)  Larger fetch responses

```properties
fetch.min.bytes=65536
fetch.max.wait.ms=500
max.poll.records=2000
fetch.max.bytes=52428800
```

Balance: larger fetches = higher throughput but higher latency.

---

## Compression Strategies

 Algorithm  Compression Ratio  CPU Cost  Best For
---------
 none  1x  None  Lowest latency
 gzip  Best  High  Storage-sensitive
 snappy  Good  Low  Balanced
 lz4  Good  Very Low  High-throughput producers
 zstd  Best  Medium  Modern choice (Kafka 2.1+)

Recommendation: **lz4** for high-throughput producers, **zstd** when storage matters.

Enable end-to-end: compress at producer, decompress only at consumer (not at broker).

---

## Consumer Lag — The Key Health Metric

**Consumer lag** = difference between latest offset and consumer committed offset.

```
Topic partition 0:
  Latest offset:     50,000
  Consumer offset:   48,500
  Lag:                1,500 messages
```

Acceptable lag depends on the use case:
- Real-time alerting: lag should be near 0
- Analytics pipeline: hours of lag may be acceptable

Monitoring tools: **Burrow**, **kafka-consumer-groups.sh**, Prometheus + Grafana.

---

## Detecting Consumer Lag

```bash
# Check lag for a consumer group
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group payment-service

GROUP            TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
payment-service  orders    0          48500           50000           1500
payment-service  orders    1          99800           100000          200
payment-service  orders    2          75000           75000           0
```

Alert when lag exceeds your SLA threshold. Investigate:
1. Is the consumer processing slowly?
2. Did producer throughput spike?
3. Is a rebalance happening?

---

## Fixing Consumer Lag

Strategies by root cause:

 Root Cause  Fix
---------
 Slow processing logic  Optimize consumer code, async processing
 Too few consumers  Add instances (up to partition count)
 Network bottleneck  Move consumers closer to brokers
 Rebalance storm  Switch to cooperative rebalancing
 Message processing errors  Fix DLQ handling, avoid retry loops
 Broker under-replicated  Add brokers, rebalance partitions

---

## Rebalancing — The Throughput Killer

**Eager rebalancing** (default before Kafka 2.3):
```
Consumer A, B, C all STOP processing
↓ All partitions revoked
↓ New assignment calculated
↓ All consumers resume
→ 100% of processing halted during rebalance (seconds to minutes!)
```

**Cooperative incremental rebalancing** (Kafka 2.4+):
```
Only affected partitions are moved
Non-affected consumers keep processing
→ Minimal disruption
```

---

## Sticky and Cooperative Rebalancing Config

```properties
# Consumer config
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

# Reduce unnecessary rebalances
session.timeout.ms=45000
heartbeat.interval.ms=15000
max.poll.interval.ms=300000

# Don't trigger rebalance on transient GC pause
```

**Static group membership** — assign a stable group.instance.id to avoid rebalance on restart:
```properties
group.instance.id=payment-service-instance-1
```

---

## Cluster Scaling Strategies

**Add brokers:**
```
Before: 3 brokers, 300 partitions (100/broker)
After:  4 brokers, need to rebalance partitions

# Use Cruise Control or kafka-reassign-partitions.sh
kafka-reassign-partitions.sh \
  --bootstrap-server kafka:9092 \
  --reassignment-json-file reassign.json \
  --execute
```

**Increase partition count:**
```bash
kafka-topics.sh --alter --topic orders \
  --partitions 12 \
  --bootstrap-server kafka:9092
```

> Warning: increasing partitions breaks key-based ordering for existing keys.

---

## Cruise Control — Automated Rebalancing

Cruise Control continuously analyzes the cluster and proposes rebalance plans:

```
Cruise Control
    │  (monitors broker metrics: CPU, disk, network, partition counts)
    ▼
Identifies imbalances:
  Broker 1: 85% disk, 90% network
  Broker 2: 30% disk, 40% network
    │
    ▼
Proposes partition moves:
  Move partition orders-5 from Broker1 to Broker2
    │
    ▼
Executes (with throttle to avoid impacting production)
```

---

## Disaster Recovery Patterns

**Backup cluster (active-passive):**
```
Primary cluster ──MirrorMaker2──► DR cluster
                                     │
                                  Failover point
                                  (if primary fails)
```

**Multi-cluster active-active:**
```
Cluster A ←──MirrorMaker2──► Cluster B
(serves region A)            (serves region B)
```

**Backup retention:**
- Keep sufficient retention in DR cluster to survive primary outage
- Test failover regularly (not just during incidents)

---

## Capacity Planning

Formula for broker storage per partition:
```
storage = message_rate × avg_message_size × retention_period × replication_factor

Example:
  message_rate = 10,000/sec
  avg_size = 1KB
  retention = 7 days
  replication = 3

  = 10,000 × 1024 × 604800 × 3
  = ~18.4 TB per topic
```

For broker count:
```
brokers = max(
    total_storage / per_broker_disk,
    total_throughput / per_broker_throughput,
    min_brokers_for_replication_factor
)
```

---

## Performance Benchmarking

Built-in tools:

**Producer benchmark:**
```bash
kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=kafka:9092 \
    batch.size=131072 \
    linger.ms=20 \
    compression.type=lz4
```

**Consumer benchmark:**
```bash
kafka-consumer-perf-test.sh \
  --bootstrap-server kafka:9092 \
  --topic perf-test \
  --messages 1000000 \
  --group perf-consumer
```

---

## Module 6 Summary

- Producer throughput: increase batch.size, linger.ms; use lz4/zstd compression
- Consumer throughput: tune fetch.min.bytes, max.poll.records
- Consumer lag is the primary health metric — monitor with Burrow
- Cooperative rebalancing dramatically reduces rebalance disruption
- Cruise Control automates cluster rebalancing and capacity management
- Capacity planning: calculate storage per topic (rate × size × retention × RF)
- Always benchmark before and after tuning changes

---

## What's Next

**Module 7 — Security & Governance**

- TLS and SASL_SSL configuration
- Kerberos, SCRAM, OAuth, RBAC
- ACLs and encryption
- Compliance and governance best practices

---

## Lab Preview — Lab 6

**Stress-Test a Kafka Cluster**

You will:
1. Baseline benchmark producer throughput with default settings
2. Tune batch size, linger.ms, and compression — compare results
3. Simulate a consumer lag spike and observe remediation
4. Trigger a rebalance and compare eager vs cooperative behavior
5. Kill a broker and observe failover and ISR changes

Environment: Docker Compose 3-broker Kafka cluster
Time: 60 minutes

---

