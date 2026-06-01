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
- Future directions in event-driven architecture

---

## Where Kafka Is Going

Kafka has evolved from a messaging system into a **universal data streaming platform**.

Current trends:
- **Serverless** — managed, elastic, consumption-based pricing
- **Edge streaming** — Kafka on constrained devices and edge gateways
- **AI integration** — streaming features for ML and real-time inference
- **Queue semantics** — Kafka 3.7+ as a work queue (not just a log)
- **Federation** — connected multi-cluster topologies across regions and clouds

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
- **Red Panda** — Kafka-compatible, low resource footprint

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
    ├── Kafka Streams (filtering, enrichment)
    ├── ksqlDB (alerting queries)
    └── S3 Sink (raw archive)
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

## Kafka Queues — Work Distribution

**Kafka 3.7+ queue semantics** for work distribution:
```
Topic: tasks (1 partition)
Consumer Group: workers (3 consumers)

  Without queue mode: all 3 consumers see all messages
  With queue mode:    each message goes to exactly one worker
                      (like RabbitMQ / SQS behavior)
```

Useful for: task queues, job distribution, worker pools — while keeping Kafka's durability and replay guarantees.

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

Limits:
  - Max 200 MB/s write, 400 MB/s read
  - Limited custom broker configs
  - 24h standard retention (tiered storage: up to years)
```

Best for: variable workloads, dev/test, new projects.

---

## Serverless Kafka — Confluent Cloud

```
Confluent Cloud (Basic / Standard / Dedicated / Enterprise):
  + Fully managed Kafka + Schema Registry + ksqlDB + Kafka Connect
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

## Module 6 Summary

- Multi-cluster federation via MirrorMaker 2 supports global, multi-cloud topologies
- Edge streaming: Kafka on Kubernetes (Strimzi) for IoT and factory use cases
- AI integration: streaming feature engineering + online inference pipelines
- Queue semantics (Kafka 3.7+) enable Kafka as a traditional work queue
- Serverless Kafka (MSK Serverless, Confluent Cloud) eliminates infrastructure management
- Modernizing legacy: strangler fig pattern with bridge connectors
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

