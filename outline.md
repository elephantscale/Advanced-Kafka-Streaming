# Advanced Kafka with Streaming Architecture

© Elephant Scale

4 Days (32 hours)

## Overview

This program is tailored for a mixed cohort — developers with some Kafka exposure and newer practitioners. It is not designed for absolute beginners.

## Delivery Model

- 4 training days, 8 hours per day
- Lecture, guided discussion, and lab practice split roughly 50/50
- Modules are ordered to build from architecture fundamentals to operations, integrations, scaling, and advanced fan-out design
- Total module time is allocated to fit a 32-hour course

## System Requirement / Lab Setup

Cognixia will provide lab access through a browser-based environment (preferred), or alternatively via RDP, with the following setup:

- Apache Kafka 4 (KRaft mode — ZooKeeper-free)
- Strimzi on Kubernetes
- Apache Flink (new addition)
- Kafka Connect
- Schema Registry (Avro/Protobuf support)
- Kafdrop along with Prometheus and Grafana

## Course Outline

---

# Module 1 — Modern Event-Driven Architecture with Kafka

**Suggested duration:** 4 hours

**Learning outcomes:**

- Explain why Kafka is used as the event backbone for modern systems
- Describe the core streaming platform components and how they work together
- Distinguish event-stream semantics from traditional queue behavior
- Position Kafka in a broader enterprise architecture

- Why Kafka is the backbone of real-time systems
  - Real-time ingestion and streaming
  - Event-driven enterprise architectures
  - Streaming-first application design
  - Where streaming beats batch for operational decisions
- Modern event-driven technology stack
  - Kafka Core
  - Kafka Connect
  - Kafka Streams
  - ksqlDB
  - Schema Registry
  - Monitoring, security, and governance layers
  - How the stack is typically deployed in enterprise environments
- Data flow walkthrough
  - Producers → Brokers → Consumers → Analytics
  - Hot path vs. cold path processing
  - Where replay and downstream enrichment fit in
- Event streams vs queue semantics
  - Retention versus deletion
  - Replay, fan-out, and independent consumer groups
- Integration with external systems
  - Spark
  - Flink
  - REST APIs
  - AI/ML pipelines
  - Cloud-native systems
  - Why Kafka often becomes the integration spine for these tools

**Hands-on Lab:** Explore cluster topology, topic naming conventions, partition layouts, and retention policies.

**Module review:** Identify the kinds of problems Kafka solves best and the architectural trade-offs it introduces.

---

# Module 2 — Kafka Internals & Cluster Architecture

**Suggested duration:** 5 hours

**Learning outcomes:**

- Explain how Kafka brokers store and replicate data
- Describe how leader election and ISR affect availability
- Understand the impact of producer and consumer tuning settings
- Relate KRaft metadata management to modern Kafka operations

- Goal: Deep operational knowledge for engineers maintaining and evolving Kafka clusters in production
- Topic 1: Optimizing Cluster Upgrade Times
  - Leader election tuning: `unclean.leader.election.enable`, controlled shutdown vs. kill, graceful shutdown strategies
  - ISR synchronization: ensuring replicas are in-sync before rolling upgrade; `min.insync.replicas` patterns; monitoring ISR shrinkage and under-replicated partitions during upgrades
  - Strimzi rolling restart: PodDisruptionBudgets, `maxUnavailable`, preferred replica election orchestration
  - Kafka 4 upgrade path: removing ZooKeeper dependency with KRaft migration
  - Upgrade sequencing and rollback considerations
- Topic 2: Broker Configuration Best Practices
  - `num.io.threads` — IO thread sizing for disk throughput
  - `num.network.threads` — network thread count, matched to producer/consumer connection load
  - `num.replica.fetchers` — replication thread count, balancing lag vs. CPU overhead
  - Heap and GC tuning
  - `acks`, `linger.ms`, `batch.size`
  - Practical tuning trade-offs for latency, throughput, and durability
- Topic 3: Internal topic behavior
  - `__consumer_offsets` and consumer group state
  - `__transaction_state` and transactional metadata
  - Why these internal topics matter when diagnosing platform issues

**Hands-on Lab:** Examine internal Kafka topics (`__consumer_offsets`, `__transaction_state`).

**Module review:** Trace how metadata, replication, and tuning choices influence cluster stability.

---

# Module 3 — Kafka Operations & Observability

**Suggested duration:** 4 hours

**Learning outcomes:**

- Select the right metrics for broker, producer, and consumer health
- Use observability tools to diagnose lag, imbalance, and replication issues
- Apply operational runbooks to common Kafka support scenarios

- Goal: Give teams the tools to run Kafka confidently and detect problems before they cause outages
- Topic 1: Key Metrics That Matter
  - Under-replicated partitions
  - Consumer lag
  - Request latency
  - ISR shrink rate
  - Disk usage and network saturation as early-warning indicators
- Topic 2: Monitoring & Tooling Stack
  - Prometheus + Grafana
  - Kafka UI
  - Kafdrop
  - Confluent Control Center
  - What each tool is best for in day-to-day operations
- Topic 3: Operational Procedures
  - Topic management: retention policies, log compaction, cleanup strategies
  - Consumer group management: reset offsets, detect stuck consumers, diagnose lag spikes
  - Security operations: TLS certificate rotation, SASL credential management
  - Audit logging for compliance: tracking producer/consumer access per topic
  - Incident triage workflow and escalation points

**Hands-on Lab:** Diagnose Kafka health using monitoring dashboards and operational runbook checks.

**Module review:** Turn raw telemetry into actionable operational decisions.

---

# Module 4 — Connectors, Pipelines & Integrations

**Suggested duration:** 5 hours

**Learning outcomes:**

- Explain how Kafka Connect simplifies integration work
- Distinguish source and sink connector responsibilities
- Describe how connectors handle offsets, retries, and failures
- Identify common enterprise integration patterns around Kafka

- Goal: Practical, hands-on integration skills for building data pipelines — from Kafka Connect through to Apache Flink
- Kafka Connect deep dive
  - Source connectors
  - Sink connectors
  - Offset management
  - Retries
  - Error handling
  - Worker model and connector lifecycle basics
- Custom connector development
- Integration patterns
  - Kafka ↔ S3
  - Kafka ↔ Elasticsearch/OpenSearch
  - Kafka ↔ Flink
  - Kafka ↔ Spark
  - Kafka ↔ NiFi
- Stream-to-batch handoff
- Backpressure management
- Enterprise integration patterns
  - Delivery guarantees and idempotency concerns
  - When to integrate directly versus through Connect

**Hands-on Lab:** Deploy and tune source and sink connectors for real-time telemetry streams.

**Module review:** Map common source and sink systems to the right connector strategy.

---

# Module 5 — Reliability, Scaling & Performance

**Suggested duration:** 6 hours

**Learning outcomes:**

- Estimate Kafka capacity for real workloads
- Scale brokers and partitions without data loss
- Tune producer, broker, and consumer settings for target SLAs
- Analyze rebalance and failover behavior under pressure

- Goal: Operational excellence for right-sizing clusters, zero-downtime scaling, and performance tuning for high-volume enterprise workloads
- Topic 1: Right-Sizing
  - Capacity planning: throughput modelling, storage sizing, retention impact
  - Partition count strategy for topic volumes (10M+ msg/sec scenarios)
  - Broker sizing: CPU, memory, disk I/O profiles for on-prem hardware
  - Benchmarking with `kafka-producer-perf-test` and `kafka-consumer-perf-test`
  - Identifying bottlenecks: network vs. disk vs. CPU bound workloads
  - How to interpret benchmark results and establish baselines
- Topic 2: Expanding Kafka with No Data Loss
  - Adding brokers to a running Kafka cluster safely
  - Partition reassignment: `kafka-reassign-partitions` without data loss
  - Throttling replication during rebalance to protect live producers
  - Strimzi-managed scaling: Kafka node pools and controlled rebalance
  - Validation: confirming ISR completeness post expansion
  - Blue/green cluster migrations for zero downtime major version upgrades
  - Planning maintenance windows and rollback paths
- Topic 3: HA & Performance Tuning
  - Replication and ISR config
  - Producer acks tuning
  - Consumer lag monitoring
  - Balancing durability against throughput and tail latency

**Hands-on Lab:** Stress-test a Kafka cluster and analyze rebalance and failover behavior.

**Module review:** Connect sizing, tuning, and resilience decisions back to business SLAs.

---

# Module 6 — Modern Kafka & Streaming Trends

**Suggested duration:** 4 hours

**Learning outcomes:**

- Recognize where Kafka is being extended beyond traditional data center patterns
- Understand the role of Kafka in edge, cloud, and AI-driven platforms
- Evaluate serverless and multi-cluster deployment models

- Multi-cluster federation and disaster recovery
- Kafka at the edge
- IoT and telecom streaming architectures
- AI-driven event processing
- Streaming inference pipelines
- Kafka queues and work-distribution patterns
- Modernizing legacy event-processing systems with Kafka
- Serverless Kafka
  - Amazon MSK Serverless
  - Confluent Cloud
- Future directions in event-driven architecture
  - What changes operationally as Kafka becomes more managed and more distributed

**Hands-on Lab:** Review emerging architecture patterns and identify where Kafka fits best in modern streaming platforms.

**Module review:** Discuss which trends are production-ready today and which are still emerging.

---

# Module 7 — High-Volume Fan-Out Best Practices

**Suggested duration:** 4 hours

**Learning outcomes:**

- Design topic layouts for high-throughput fan-out scenarios
- Reduce unnecessary downstream processing with better filtering strategies
- Use stream processing and autoscaling patterns to support many consumers

- Scenario: A dataset is streamed at 10 million messages per second. 10 unique consumers each need overlapping but different subsets of the data. Goal: minimize storage/network duplication on Kafka and minimize consumer CPU overhead for filtering unwanted messages.
- Topic 1: Topic Design Strategies
  - Single broad topic vs. pre-filtered sub-topics: cost-benefit analysis at 10M/s scale
  - Partition key design to co-locate related data for consumer efficiency
  - Topic compaction + header-based routing for lightweight filtering
  - Shared subscription patterns and consumer group topology for 10 overlapping consumers
  - When to prefer one topic with filtering versus multiple derived topics
- Topic 2: Efficient Filtering Approaches
  - Header-based filtering
  - Schema-based filtering
  - Kafka Streams branching
  - ksqlDB materialized views
  - Trade-offs in CPU, network, and operational complexity
- Lab Exercises
  - Architecture design exercise for a 10M/s, 10-consumer overlapping subset scenario
  - Header-based filtering pipeline implementation
  - Benchmark: duplication vs. filtering
  - KEDA autoscaler configuration tied to Prometheus consumer lag metric
  - Validate consumer scale-up and scale-down behavior under simulated load

**Hands-on Lab:** Design and validate a fan-out strategy that balances throughput, filtering cost, and consumer scalability.

**Module review:** Choose the right fan-out strategy for a real production workload.

---
