# Module 9 — Modern Kafka & Streaming Trends

Elephant Scale

---

## Module 9 Agenda

- Multi-cluster federation and disaster recovery
- Kafka at the edge
- IoT and telecom streaming architectures
- AI-driven event processing
- Streaming inference pipelines
- Kafka queues and work-distribution patterns
- Modernizing legacy event-processing systems
- Serverless Kafka

---

## Where Kafka Is Going

Kafka has evolved from a messaging system into a **universal data streaming platform**.

Current trends:
- **Serverless** — managed, elastic, consumption-based pricing
- **Edge streaming** — Kafka on constrained devices and edge nodes
- **AI integration** — streaming features for ML, real-time inference
- **Queue semantics** — Kafka as a work queue (not just a log)
- **Federation** — connected multi-cluster topologies

---

## Multi-Cluster Federation

Modern enterprises run multiple Kafka clusters:

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

Use cases:
- Data locality (GDPR, latency)
- Blast radius containment
- Multi-cloud strategy
- Organization boundaries

---

## Federation Patterns

**Hub and spoke:**
```
Regional Clusters → Central Hub Cluster → Analytics
(edge data collection)  (global view)   (reporting)
```

**Mesh topology:**
```
Cluster A ←→ Cluster B
    ↕              ↕
Cluster C ←→ Cluster D
```

**Selective replication:**
```
Only replicate topics with policy=global
Local topics stay local (reduce bandwidth, improve privacy)
```

---

## Kafka at the Edge

Edge computing brings processing closer to data sources:

```
Factory Floor / IoT Device
    │  (edge Kafka, limited resources)
    │  collects sensor data locally
    ▼
Edge Gateway (small Kafka cluster)
    │  aggregates + filters
    │  forwards relevant events
    ▼
Cloud/Data Center Kafka Cluster
    │  full processing, ML, analytics
    ▼
Enterprise Systems
```

Implementations:
- **Red Panda** — Kafka-compatible, low resource footprint
- **Strimzi** — Kafka on Kubernetes, edge clusters
- **Apache Pulsar** — alternative with lightweight bookkeeper

---

## IoT Streaming Architecture

```
Devices (millions)
    │  MQTT / CoAP / HTTP
    ▼
MQTT Broker (Eclipse Mosquitto)
    │  MQTT-Kafka bridge
    ▼
Kafka (raw telemetry topics)
    │
    ├── Kafka Streams (real-time filtering, enrichment)
    ├── ksqlDB (alerting queries)
    └── S3 Sink (raw archive)
            │
            ▼
    Analytics (Spark, Flink, Athena)
```

---

## IoT Design Considerations

 Challenge  Solution
--------
 Millions of devices  Partition by device_id, large partition count
 Intermittent connectivity  MQTT QoS 1/2, idempotent producers
 Protocol diversity (MQTT, CoAP)  Edge protocol gateways
 Device time drift  Always use event timestamp extraction
 Data volume  Edge filtering + compression before forwarding
 Device identity  mTLS certificates per device

---

## Telecom Streaming Architecture

Telcos use Kafka for:
- **CDR (Call Detail Records)** — billions/day
- **Network telemetry** — 5G RAN metrics
- **Fraud detection** — real-time call pattern analysis
- **Network management** — topology change events

```
Network Elements (routers, RAN, core)
    │  SNMP, NETCONF, gRPC streaming
    ▼
Telemetry Collection Layer
    │
    ▼
Kafka (10M+ events/sec at scale)
    │
    ├── Real-time anomaly detection (Flink)
    ├── Fraud detection (Kafka Streams ML)
    └── Data lake (S3/HDFS)
```

---

## AI-Driven Event Processing

Kafka enables **real-time AI at every step** of the pipeline:

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
  [Downstream Actions]   ← routing, alerts, A/B test decisions
```

---

## Streaming Feature Engineering

```python
# Kafka Streams Python (faust)
app = faust.App('feature-engineering', broker='kafka://kafka:9092')

raw_orders = app.topic('raw-orders', value_type=Order)
features = app.topic('order-features', value_type=OrderFeatures)

@app.agent(raw_orders)
async def compute_features(orders):
    async for order in orders:
        feature = OrderFeatures(
            order_id=order.id,
            amount_log=math.log(order.amount + 1),
            hour_of_day=order.timestamp.hour,
            is_weekend=order.timestamp.weekday() >= 5,
            customer_avg_order=await get_customer_avg(order.customer_id)
        )
        await features.send(key=order.id, value=feature)
```

---

## Streaming ML Inference

Two patterns for online inference:

**Pattern 1: Call external model server per event:**
```python
@app.agent(feature_topic)
async def infer(features):
    async for feature in features:
        score = await model_server.predict(feature)  # HTTP to TF Serving / Triton
        await predictions.send(key=feature.order_id, value=Prediction(score=score))
```

**Pattern 2: Embed model in Kafka Streams app:**
```java
// Load ONNX model inside Kafka Streams processor
OrtSession model = OrtEnvironment.getEnvironment().createSession("fraud_model.onnx");
float score = runInference(model, featureVector);
```

Pattern 2: lower latency, no network hop. Pattern 1: model updates without redeployment.

---

## Kafka Queues — Work Distribution

**Kafka 3.7+ introduced queue semantics** for work distribution:

Traditional queue behavior (new):
- Multiple consumers in a group **each get unique messages** (no duplicates)
- Message is "deleted" after one consumer processes it
- Useful for task queues, job distribution

```
Topic: tasks (1 partition)
Consumer Group: workers (3 consumers)

  Without queue mode: all 3 see all messages
  With queue mode:    each message goes to exactly one worker
                      (like RabbitMQ/SQS behavior)
```

---

## Modernizing Legacy Event Processing

Many enterprises have legacy systems (MQ, TIBCO, IBM MQ):

**Migration path:**
```
Legacy MQ
    │  (bridge connector: IBM MQ → Kafka)
    ▼
Kafka
    │  (new consumers read from Kafka)
    ▼
Modern microservices

# Legacy systems continue sending to MQ
# Bridge replicates to Kafka
# New services read from Kafka (no legacy dependency)
```

Strangle-fig pattern: gradually replace legacy consumers, keeping legacy producers running.

---

## Serverless Kafka — Amazon MSK Serverless

```
MSK Serverless:
  - No cluster provisioning
  - Auto-scales capacity
  - Pay per throughput (not per broker-hour)
  - Kafka API compatible (no code changes)

Limits:
  - Max 200 MB/s write, 400 MB/s read
  - Max retention: 24 hours standard (tiered storage: up to years)
  - No custom broker configs
```

Best for: variable workloads, dev/test, new projects.
Not for: extremely high throughput, low-latency requirements, custom broker config needs.

---

## Serverless Kafka — Confluent Cloud

```
Confluent Cloud (Basic/Standard/Dedicated/Enterprise):
  - Fully managed Kafka + Schema Registry + ksqlDB + Kafka Connect
  - Stream Governance (data catalog, lineage)
  - Multi-cloud (AWS, GCP, Azure)
  - Consumption-based pricing on Basic/Standard
```

New: **Tableflow** — Kafka topics as Apache Iceberg tables (direct query from Spark/Athena).

---

## The Future of Event-Driven Architecture

Emerging directions:
- **Kafka + Iceberg** — streaming-native data lakehouse (no ETL)
- **Real-time AI agents** — event-triggered autonomous agents
- **Unified batch/streaming** — Apache Flink Table API, Spark Streaming
- **AI-native pipelines** — embedding LLMs in stream processors
- **Decentralized data mesh** — Kafka as the event backbone of data mesh

---

## Module 9 Summary

- Multi-cluster federation via MirrorMaker 2 supports global, multi-cloud topologies
- Edge streaming brings Kafka to constrained environments (IoT, factory, telco)
- AI integration: streaming feature engineering + online inference pipelines
- Queue semantics (Kafka 3.7+) enable Kafka as a traditional work queue
- Serverless Kafka (MSK Serverless, Confluent Cloud) eliminates infrastructure management
- The future: Kafka + Iceberg, AI-native pipelines, data mesh

---

## What's Next

**Module 10 — Capstone & Best Practices**

- Architecture review of a real enterprise streaming use case
- End-to-end design workshop
- Design checklist: security, scalability, reliability, observability, governance
- Best practices review

---

## Lab Preview — Lab 9

**Explore Modern Kafka Integrations**

You will:
1. Simulate an IoT telemetry pipeline with MQTT → Kafka bridge
2. Build a streaming feature engineering pipeline with Faust
3. Deploy a Kafka Streams application that calls an ML inference endpoint
4. Explore MSK Serverless or Confluent Cloud (instructor-provided credentials)

Environment: Docker Compose (Kafka, MQTT broker, ML mock server)
Time: 60 minutes

---

