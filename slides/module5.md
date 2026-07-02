# Module 5 — Reliability, Scaling & Performance

Elephant Scale

---

## Module 5 Agenda

- Right-sizing Kafka clusters
- Expanding Kafka with no data loss
- HA and performance tuning
- Producer and consumer throughput parameters
- Consumer lag detection and remediation
- Cooperative rebalancing
- Broker failure, failover, and ISR recovery
- Capacity planning formula

---

## What Limits Kafka Throughput?

1. **Network bandwidth** — bytes in/out per broker
2. **Disk I/O** — sequential write throughput
3. **CPU** — compression/decompression, serialization
4. **Producer batching** — small batches = more overhead
5. **Replication factor** — more replicas = more network traffic
6. **Consumer parallelism** — limited by partition count

> In practice: network and disk are the most common bottlenecks.

---

## Right-Sizing: Capacity Planning Formula

```
storage_per_topic =
    message_rate × avg_message_size × retention_days × replication_factor

Example:
    10,000 msg/sec × 1KB × 7 days × 3 replicas
    = 10,000 × 1,024 × 604,800 × 3
    ≈ 18.4 TB
```

Broker count:
```
brokers = max(
    total_storage / per_broker_disk,
    total_throughput / per_broker_throughput,
    min_brokers_for_replication_factor
)
```

> **Tiered Storage (KIP-405, GA in Kafka 4) changes this math.** Only the *hot* tail
> (`local.retention.ms`) stays on broker disk; older segments offload to object storage.
> Long-retention topics no longer size the cluster on broker disk — broker count is then
> driven by *throughput*, not total storage. Re-run the formula with `local.retention`
> in place of full `retention`.

---

## Producer Throughput Tuning

| Parameter | Tuning Direction | Effect |
|---|---|---|
| `batch.size` | Increase (64KB–1MB) | Larger batches, fewer requests |
| `linger.ms` | Increase (5–100ms) | Wait to fill batches |
| `compression.type` | `lz4` or `zstd` | Smaller network payload |
| `buffer.memory` | Increase (64MB+) | More in-flight data |

```properties
batch.size=131072
linger.ms=20
compression.type=lz4
buffer.memory=67108864
```

---

## Consumer Throughput Tuning

| Parameter | Effect |
|---|---|
| `fetch.min.bytes` | Wait for more data per fetch |
| `fetch.max.wait.ms` | Max wait time for min bytes |
| `max.poll.records` | Records processed per poll |
| `fetch.max.bytes` | Larger fetch responses |

```properties
fetch.min.bytes=65536
fetch.max.wait.ms=500
max.poll.records=2000
fetch.max.bytes=52428800
```

---

## Compression Strategy

| Algorithm | Ratio | CPU Cost | Best For |
|---|---|---|---|
| none | 1× | None | Lowest latency |
| lz4 | Good | Very Low | High-throughput producers |
| zstd | Best | Medium | Modern choice, storage-sensitive |
| gzip | Best | High | Legacy compatibility |
| snappy | Good | Low | Balanced |

Enable end-to-end: compress at producer, decompress only at consumer.

---

## Expanding Kafka with No Data Loss

```bash
# Step 1: Generate reassignment plan
kafka-reassign-partitions.sh \
  --bootstrap-server kafka:9092 \
  --topics-to-move-json-file topics.json \
  --broker-list "1,2,3,4" \
  --generate

# Step 2: Execute with throttle (protect live producers)
kafka-reassign-partitions.sh \
  --bootstrap-server kafka:9092 \
  --reassignment-json-file reassign.json \
  --throttle 52428800 \
  --execute

# Step 3: Verify ISR completeness
kafka-reassign-partitions.sh \
  --bootstrap-server kafka:9092 \
  --reassignment-json-file reassign.json \
  --verify
```

---

## Blue/Green Cluster Migration

```
Old Cluster ──MirrorMaker2──► New Cluster
                                  │
                              (validate)
                                  │
                          Switch producers → New Cluster
                                  │
                          Wait for lag to drain
                                  │
                          Switch consumers → New Cluster
                                  │
                          Decommission old cluster
```

Zero-downtime: producers switch first, consumers drain, then switch.

---

## Consumer Lag, Intuitively

Lag is just the **checkout line** at a supermarket: messages arrive, consumers ring them
up, and lag = the shoppers still waiting.

- Lag **grows** whenever **arrival rate > processing rate** — the line backs up.
- Two ways to shrink it: **process faster** (optimize, go async) or **add cashiers**
  (more consumers) — but only up to the number of **lanes** (partitions).
- Steady non-zero lag is fine. **Steadily *rising* lag** is the alarm.

> That's why lag is *the* health metric: the earliest, clearest sign your consumers can't
> keep up with your producers.

---

## Consumer Lag — The Primary Health Metric

```
Topic partition 0:
  Latest offset:     50,000
  Consumer offset:   48,500
  Lag:                1,500 messages
```

Root cause vs fix:

| Root Cause | Fix |
|---|---|
| Slow processing logic | Optimize code, async processing |
| Too few consumers | Add instances (up to partition count) |
| Rebalance storm | Switch to cooperative rebalancing |
| Message errors | DLQ handling, avoid retry loops |

---

## Rebalancing — The Throughput Killer

**Eager rebalancing** (default before Kafka 2.3):
```
All consumers STOP → all partitions revoked → new assignment → all resume
→ 100% processing halted (seconds to minutes)
```

**Cooperative incremental rebalancing** (client-side, Kafka 2.4+):
```
Only affected partitions move
Non-affected consumers keep processing
→ Minimal disruption
```

Config:
```properties
partition.assignment.strategy=CooperativeStickyAssignor
group.instance.id=payment-service-instance-1   # static membership
```

**New consumer protocol — KIP-848 (GA in Kafka 4):**
```
The BROKER computes and drives the assignment (no client-side leader,
no JoinGroup/SyncGroup stop-the-world barrier).
→ Incremental by design; faster, more stable rebalances at scale
```
```properties
group.protocol=consumer        # opt in to the new protocol
```
> The classic protocol still works; KIP-848 is the forward path. Lab 5 compares all three.

---

## When Rebalancing Goes Wrong — The Rebalance Storm

A real production failure pattern:

- A 30-consumer group processes payments. One pod hits a long GC pause and misses a
  heartbeat → the group **rebalances** → under the eager protocol, **all 30 stop**.
- The pause only *looked* like death; the pod rejoins → **another rebalance** → everyone
  stops again.
- Under load this loops: the group spends more time **rebalancing than processing**. Lag
  explodes, latency spikes, on-call gets paged.

The fix is exactly the config from the previous slide:
- **Cooperative rebalancing** — only affected partitions move; the other 29 keep working.
- **Static membership** (`group.instance.id`) — a brief restart no longer triggers a reassignment.
- **KIP-848** — the broker drives incremental assignment; no stop-the-world barrier.

> The "rebalance storm" is one of the most common Kafka production incidents. Recognizing
> it — and knowing these three fixes — turns a 3-hour outage into a 5-minute triage.

---

## Broker Failure and ISR Recovery

```
Normal state:
  Partition 0: Leader=B1, ISR=[B1, B2, B3]

B2 fails:
  Partition 0: Leader=B1, ISR=[B1, B3]  ← URP!

B2 restarts and catches up:
  Partition 0: Leader=B1, ISR=[B1, B2, B3]  ← restored
```

Monitor `UnderReplicatedPartitions` — non-zero is the first sign of trouble.

Key configs:
- `replica.lag.time.max.ms` — how long before a replica is removed from ISR
- `min.insync.replicas` — minimum ISR required for produce success

---

## HA Configuration Matrix

| Scenario | `acks` | `min.insync.replicas` | Result |
|---|---|---|---|
| Maximum durability | `all` | 2 | No data loss, some throughput cost |
| High throughput | `1` | 1 | Possible data loss on broker failure |
| Zero data loss | `all` | RF (e.g. 3) | Writes fail if any broker is down |

Recommendation for production: `acks=all`, `min.insync.replicas=2`, `replication-factor=3`.

---

## Module 5 Summary

- Capacity planning: model storage per topic before provisioning
- Expanding Kafka: use `kafka-reassign-partitions` with throttle, validate ISR
- Producer tuning: larger batches, linger, lz4/zstd compression
- Consumer tuning: fetch.min.bytes, max.poll.records
- Consumer lag is the primary operational health metric
- Cooperative rebalancing dramatically reduces disruption; the KIP-848 broker-driven protocol is the Kafka 4 forward path
- Tiered Storage decouples retention from broker disk — size on throughput, not total storage
- HA: `acks=all`, `min.insync.replicas=2`, `replication-factor=3`

---

## What's Next

**Module 6 — Modern Kafka & Streaming Trends**

- Multi-cluster federation and disaster recovery
- Kafka at the edge and IoT
- AI-driven streaming pipelines
- Serverless Kafka: MSK Serverless, Confluent Cloud

---

## Lab Preview — Lab 5

**Stress-Test a Kafka Cluster and Analyze Rebalance and Failover Behavior**

You will:
1. Baseline and tuned throughput benchmarks
2. Consumer lag simulation and group-based remediation
3. Eager vs cooperative rebalancing comparison
4. Partition reassignment with throttling on a live cluster
5. Broker failure simulation with ISR recovery measurement
6. End-to-end latency measurement

Environment: Docker Compose (3-broker Kafka cluster)
Time: 75–90 minutes

---

