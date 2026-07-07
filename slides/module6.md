# Module 6 — Modern Kafka & Streaming Trends

Elephant Scale

---

## Module 6 Agenda

- Where Kafka is going
- Multi-cluster federation and disaster recovery
- Kafka at the edge and IoT architectures
- AI-driven event processing and streaming inference
- Kafka queues and work-distribution patterns
- Modernizing legacy event-processing systems
- Serverless Kafka: MSK Serverless and Confluent Cloud
- Upgrading Kafka in the KRaft era: safe, fast rolling upgrades
- Future directions in event-driven architecture

---

## Where Kafka Is Going

Kafka has evolved from a messaging system into a **universal data streaming platform**.

Current trends:
- **Serverless** — managed, elastic, consumption-based pricing
- **Diskless / object-store Kafka** — brokers backed directly by S3-class storage (KIP-1150 "diskless topics"; WarpStream, AutoMQ, Confluent Freight) — cheaper, near-infinite retention, no local disks to manage
- **Edge streaming** — Kafka on constrained devices and edge gateways
- **AI integration** — streaming features for ML and real-time inference
- **Native queue semantics** — Share Groups (KIP-932, Kafka 4) make Kafka a work queue, not just a log
- **Federation** — connected multi-cluster topologies across regions and clouds

---

## Trends: Ready Now vs Emerging vs Horizon

A trends module is only useful if you know **what to bet on today** vs what to just watch:

- **Production-ready now** — MirrorMaker 2 federation · serverless (MSK Serverless,
  Confluent Cloud) · **Share Groups** (KIP-932, early access but usable) · Kafka → Iceberg sinks.
- **Emerging (pilot, not core yet)** — **diskless / object-store Kafka** (KIP-1150;
  WarpStream, AutoMQ) · Flink-native lakehouse (Tableflow) · edge Kafka (Redpanda / Strimzi).
- **Horizon (watch, don't build on)** — real-time AI agents · LLMs embedded in stream
  processors · decentralized data mesh.

> Rule of thumb: make architecture decisions on the "ready now" column; treat "horizon" as
> informed peripheral vision, not a roadmap.

---

## Multi-Cluster Federation

```
┌────────────────────────────────────────────────────┐
│                 Global Federation                   │
│                                                     │
│  Cluster: us-east-1  ←──MM2──►  Cluster: eu-west-1  │
│       │                               │             │
│       └──────────MM2──────────────────┘             │
│                   │                                 │
│           Cluster: ap-southeast-1                   │
└────────────────────────────────────────────────────┘
```

Use cases: data locality (GDPR, latency), blast radius containment, multi-cloud strategy, organizational boundaries.

---

## Federation Patterns

**Hub and spoke:**
```
Regional Clusters → Central Hub Cluster → Analytics
(edge data collection)  (global view)   (reporting)
```

**Selective replication:**
```
Only replicate topics with policy=global
Local topics stay local (reduce bandwidth, improve privacy)
```

**Active-active DR:**
```
Primary cluster ←──MirrorMaker2──► DR cluster
(reads + writes)                   (reads + writes)
```

---

## Kafka at the Edge

```
Factory Floor / IoT Devices
    │  (edge Kafka, limited resources)
    ▼
Edge Gateway (small Kafka cluster)
    │  aggregates + filters
    ▼
Cloud/Data Center Kafka Cluster
    │  full processing, ML, analytics
    ▼
Enterprise Systems
```

Implementations:
- **Strimzi** — Kafka on Kubernetes, edge clusters
- **Redpanda** — Kafka-compatible, single-binary, low resource footprint

---

## IoT Streaming Architecture

```
Devices (millions)
    │  MQTT / CoAP / HTTP
    ▼
MQTT Broker
    │  MQTT-Kafka bridge
    ▼
Kafka (raw telemetry topics)
    │
    ├── Kafka Streams / Flink (filtering, enrichment)
    ├── Flink SQL (alerting queries)
    └── S3 / Iceberg Sink (raw archive)
```

Key design considerations: partition by `device_id`, idempotent producers, mTLS per device, edge filtering before forwarding.

---

## AI-Driven Event Processing

```
Raw Events
    │
  [Feature Engineering]  ← Kafka Streams: compute features in real time
    │
  [Feature Store Topic]  ← materialized as KTable
    │
  [Model Inference]      ← call ML endpoint per event (or embedded model)
    │
  [Prediction Topic]     ← inference results as events
    │
  [Downstream Actions]   ← routing, alerts, A/B decisions
```

---

## Streaming ML Inference: Two Patterns

**Pattern 1: External model server per event:**
```python
score = await model_server.predict(feature)  # HTTP to TF Serving / Triton
await predictions.send(key=id, value=Prediction(score=score))
```

**Pattern 2: Embedded model in stream processor:**
```java
OrtSession model = OrtEnvironment.getEnvironment()
    .createSession("fraud_model.onnx");
float score = runInference(model, featureVector);
```

Pattern 2: lower latency, no network hop.
Pattern 1: model updates without redeploying the stream application.

---

## Kafka Queues — Share Groups (KIP-932)

**Native queue semantics arrived in Kafka 4.0** as Share Groups (early access):
```
Topic: tasks (any partition count)
Share Group: workers (N share consumers)

  Consumer group:  each partition is owned by exactly one consumer
                   (parallelism capped at partition count)
  Share group:     many consumers pull from the SAME partitions,
                   each record acknowledged individually
                   (like RabbitMQ / SQS — but durable and replayable)
```

- Consumer count is **decoupled from partition count** — scale workers past partitions
- Per-message ack/redelivery; tooling: `kafka-console-share-consumer.sh`, `kafka-share-groups.sh`
- Useful for: task queues, job distribution, worker pools — while keeping Kafka's durability and replay

---

## Modernizing Legacy Event Processing

```
Legacy MQ (IBM MQ, TIBCO)
    │  (bridge connector)
    ▼
Kafka
    │
    ├── New microservices (read from Kafka)
    └── Legacy consumers (still reading from MQ)
```

**Strangler fig pattern:** gradually replace legacy consumers.
New services read from Kafka. Legacy producers keep sending to MQ. Bridge replicates to Kafka.
No big-bang migration required.

---

## Serverless Kafka — Amazon MSK Serverless

```
MSK Serverless:
  + No cluster provisioning
  + Auto-scales capacity
  + Pay per throughput (not per broker-hour)
  + Kafka API compatible — no code changes needed

Limits (verify current values in AWS docs — quotas change):
  - Per-cluster ingress/egress throughput caps
  - Limited custom broker configs
  - Partition-per-cluster quota
  - Retention bounded by the managed storage tier
```

Best for: variable workloads, dev/test, new projects.

---

## Serverless Kafka — Confluent Cloud

```
Confluent Cloud (Basic / Standard / Dedicated / Enterprise):
  + Fully managed Kafka + Schema Registry + Flink + Kafka Connect
  + Stream Governance: data catalog, lineage tracking
  + Multi-cloud (AWS, GCP, Azure)
  + Tableflow: Kafka topics as Apache Iceberg tables
    → direct query from Spark / Athena (no ETL)
```

Consumption-based pricing on Basic/Standard tiers.

---

## Serverless Readiness Checklist

Before migrating to a managed/serverless offering:

- ☐ No broker-specific client assumptions
- ☐ Retries and idempotence enabled
- ☐ Topic and ACL provisioning scripted (not manual)
- ☐ Metrics exported externally (not broker shell scraping)
- ☐ Schema Registry usage standardized
- ☐ Cost model understood (traffic + retention pricing)
- ☐ No reliance on ZooKeeper or self-managed KRaft config

---

## The Future of Event-Driven Architecture

- **Kafka + Iceberg** — streaming-native data lakehouse (no ETL)
- **Real-time AI agents** — event-triggered autonomous agents responding to streams
- **Unified batch/streaming** — Apache Flink Table API, Spark Streaming convergence
- **AI-native pipelines** — embedding LLMs in stream processors
- **Decentralized data mesh** — Kafka as the event backbone of data mesh

---

## Upgrading Kafka in the KRaft Era

Upgrades are where availability promises are tested. KRaft (Kafka 4, ZooKeeper-free) **simplified the upgrade story** — one system to upgrade, not two — but the discipline is unchanged: **never lose the controller quorum, never drop a partition below `min.insync.replicas`.**

What KRaft changed:
- **No ZooKeeper** — there is no separate ensemble to upgrade first; brokers and controllers are the whole system.
- **Feature flags via `metadata.version`** — cluster capabilities are gated by a metadata level you raise *after* every node runs the new binary (the modern replacement for `inter.broker.protocol.version`, managed with `kafka-features.sh`).
- **Controllers roll like brokers** — the KRaft controller quorum upgrades one node at a time, always preserving the majority.

> Goal: a **zero-downtime rolling upgrade** — clients never notice, because a healthy cluster always keeps a leader and a full ISR for every partition.

![Rolling upgrade](../images/placeholder-rolling-upgrade.png)

Notes:
The point to land: KRaft made upgrades *simpler* — one system to upgrade, not two — but the safety rule is unchanged. Frame an upgrade as just a controlled series of broker restarts: everything they learned about ISR and min.insync.replicas in Module 5 is exactly what keeps it zero-downtime. Quick engagement — ask the room: who's run a rolling upgrade, and what went wrong?

---

## Rolling Upgrade — The Safe Procedure

Upgrade **one node at a time**, and only advance when the cluster is fully healthy again:

1. **Roll the controllers first**, one at a time — keep the quorum majority online throughout.
2. For each **broker**: stop it, install the new binary, restart, then **wait for `UnderReplicatedPartitions = 0`** and the ISR to fully re-expand before touching the next node.
3. Once **every** node runs the new version, **raise `metadata.version`** (`kafka-features.sh upgrade`) to enable the new features — never before.
4. Run a **preferred-leader election** afterward to rebalance leadership evenly across brokers.

> The wait-for-URP-zero step is the whole game: skip it and you can take two replicas of the same partition down at once → offline partition → downtime.

Notes:
Walk the four steps slowly — this is the procedure they'll actually run in production. Emphasize step 2's wait-for-URP-zero: that discipline is what separates a clean upgrade from an outage. The classic failure is impatience — restarting the next broker before the previous one's replicas caught up, taking two replicas of the same partition down together. And metadata.version last, never before: enabling a feature the still-old binaries don't understand will break them.

---

## Optimizing Upgrade Time & Availability

Faster and safer are **not** in tension — the same design gives you both:

- **`min.insync.replicas=2` + `acks=all`** — a single broker down (the one being upgraded) still accepts writes, so the roll never blocks producers.
- **RF ≥ 3** — tolerate the upgrading broker being offline with room to spare.
- **Watch three signals** through the roll: **URP** (must return to 0 before the next node), **consumer lag** (should stay flat), **request latency** (brief blips are fine).
- **Parallelize safely** — brokers in different racks/AZs can roll a bit faster, but **never two replicas of the same partition** at once.
- **Automate it** — Strimzi (Kubernetes) and Cruise Control run the roll-and-wait loop for you, removing human error and cutting total upgrade time.

> The single biggest time-saver: proper replication + `min.insync.replicas` lets you roll **without draining traffic** — the cluster carries full load the entire time.

Notes:
The reframe to sell: fast and safe are not opposites. With RF=3 and min.insync.replicas=2 you roll under full load — no maintenance window, no draining traffic. That IS the optimization. The three signals (URP, lag, latency) are the go/no-go gate between nodes. Close on automation: nobody should hand-roll a 30-broker cluster — Strimzi and Cruise Control run the wait-and-watch loop for you and cut total upgrade time.

---

## Module 6 Summary

- Multi-cluster federation via MirrorMaker 2 supports global, multi-cloud topologies
- Edge streaming: Kafka on Kubernetes (Strimzi) for IoT and factory use cases
- AI integration: streaming feature engineering + online inference pipelines
- Native queue semantics — Share Groups (KIP-932, Kafka 4) — enable Kafka as a traditional work queue, decoupled from partition count
- Serverless Kafka (MSK Serverless, Confluent Cloud) eliminates infrastructure management
- Modernizing legacy: strangler fig pattern with bridge connectors
- KRaft-era upgrades roll one node at a time, waiting for URP=0; `min.insync.replicas`+`acks=all` keep it zero-downtime
- Future: Kafka + Iceberg, AI-native pipelines, data mesh

---

## What's Next

**Module 7 — High-Volume Fan-Out Best Practices**

- Topic layout strategies for 10M msg/sec with 10 overlapping consumers
- Header-based filtering
- Kafka Streams branching
- KEDA autoscaling tied to consumer lag

---

## Lab Preview — Lab 6

**Review Emerging Architecture Patterns**

You will:
1. Build a lightweight edge-to-core event pipeline with filtering
2. Implement a real-time feature enrichment stage
3. Integrate a mock inference service with Kafka events
4. Demonstrate queue-style work distribution
5. Assess a pipeline's serverless readiness

Environment: Docker Compose (Kafka, Python, Flask mock model)
Time: 60 minutes

---

