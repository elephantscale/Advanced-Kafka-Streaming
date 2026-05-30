# Advanced Apache Kafka Streaming

© Elephant Scale

May 13, 2026

## Overview

The goal of this training is to give engineers and architects deep mastery of Kafka internals, operational reliability, and modern event-driven architecture practices, enabling them to design, deploy, and optimize enterprise-grade streaming platforms in production.

### Learning Objectives

By the end of this training, participants will be able to:

- Understand modern Kafka-based event-driven architectures and enterprise streaming evolution
- Apply advanced Kafka design and tuning patterns
- Manage reliability, scaling, and security in high-volume clusters
- Build, deploy, and monitor real-time streaming applications
- Integrate event-driven data pipelines with enterprise and cloud systems
- Enforce governance and observability best practices across environments
- Understand modern queue semantics and streaming convergence in Kafka

### Prerequisites

- Strong understanding of Kafka fundamentals (topics, producers, consumers)
- Experience with Linux command line and Docker/Kubernetes environments
- Familiarity with streaming use cases (IoT, analytics, observability, data integration)

---

# Audience

- Senior data engineers
- Platform architects
- DevOps engineers
- Streaming platform leads
- Site reliability engineers (SREs)

---

# Duration

4 days

---

# Format

Lectures and hands-on labs (50% lecture / 50% labs)

---

# Lab Environment

A cloud-based lab environment will be provided.

---

# Students Will Need

- A modern laptop with unrestricted Internet access
- Chrome browser
- SSH client for their platform

---

# Advanced Kafka Training Outline

---

# Module 1 — Modern Event-Driven Architecture with Kafka

- Why Kafka is the backbone of real-time systems
  - Real-time ingestion and streaming
  - Event-driven enterprise architectures
  - Streaming-first application design
- Modern event-driven technology stack
  - Kafka Core
  - Kafka Connect
  - Kafka Streams
  - ksqlDB
  - Schema Registry
  - Monitoring, security, and governance layers
- Data flow walkthrough
  - Producers → Brokers → Consumers → Analytics
- Event streams vs queue semantics
- Integration with external systems
  - Spark
  - Flink
  - REST APIs
  - AI/ML pipelines
  - Cloud-native systems

**Hands-on Lab:**  
Explore cluster topology, topic naming conventions, partition layouts, and retention policies.

---

# Module 2 — Kafka Internals & Cluster Architecture

- Broker internals
  - Log segments
  - Indexes
  - Compaction
  - Storage architecture
- ZooKeeper-free Kafka architecture (KRaft)
- Controller quorum and metadata management
- Partition assignment and leader election
- In-Sync Replicas (ISR)
- Producer internals
  - Batching
  - Acknowledgments
  - Compression
  - Idempotence
- Consumer internals
  - Group coordination
  - Rebalancing
  - Offset management
- Exactly-once semantics and transactions

**Hands-on Lab:**  
Examine internal Kafka topics (`__consumer_offsets`, `__transaction_state`).

---

# Module 3 — Advanced Topic Design & Data Modeling

- Partition design and key selection
- Avoiding data skew and hot partitions
- Topic compaction vs deletion
- Event schema design
- Schema evolution and compatibility
  - Avro
  - Protobuf
  - JSON Schema
- Tiered storage and topic lifecycle management
- Cross-cluster replication
  - MirrorMaker 2
  - Replicator
- Multi-region streaming architectures

**Hands-on Lab:**  
Create and benchmark high-throughput topics under varying workloads.

---

# Module 4 — Stream Processing with Kafka Streams & ksqlDB

- Stateless vs stateful transformations
- Joins, aggregations, and windowing
  - Tumbling windows
  - Hopping windows
  - Sliding windows
- Handling out-of-order and late-arriving data
- State stores and fault tolerance
- Scaling stream processing applications
- Monitoring and debugging stream topologies
- Event-driven AI and anomaly detection pipelines

**Hands-on Lab:**  
Build a Kafka Streams application to detect anomalies in real-time telemetry streams.

---

# Module 5 — Connectors, Pipelines & Integrations

- Kafka Connect deep dive
  - Source connectors
  - Sink connectors
  - Offset management
  - Retries
  - Error handling
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

**Hands-on Lab:**  
Deploy and tune source and sink connectors for real-time telemetry streams.

---

# Module 6 — Reliability, Scaling & Performance

- Throughput tuning parameters
  - `linger.ms`
  - `batch.size`
  - `fetch.min.bytes`
  - Compression strategies
- Detecting and fixing consumer lag
- Cooperative and sticky rebalancing
- Cluster scaling strategies
- Tiered storage optimization
- Disaster recovery and failover patterns
- Capacity planning
- Performance benchmarking methodologies

**Hands-on Lab:**  
Stress-test a Kafka cluster and analyze rebalance and failover behavior.

---

# Module 7 — Security & Governance

- TLS and SASL_SSL configuration
- Authentication mechanisms
  - Kerberos
  - SCRAM
  - OAuth
  - RBAC
- Authorization via ACLs
- Encryption and secrets management
- Governance best practices
  - Topic ownership
  - Naming conventions
  - Retention governance
  - PII masking
- Security posture for enterprise Kafka deployments
- Compliance considerations

**Hands-on Lab:**  
Configure ACLs and validate producer/consumer access control.

---

# Module 8 — Observability & Operations

- Core Kafka metrics
  - Broker metrics
  - Producer metrics
  - Consumer metrics
- Monitoring tools
  - Prometheus
  - Grafana
  - Burrow
  - Cruise Control
- Alerting and anomaly detection
- Rolling upgrades and restarts
- Troubleshooting production issues
  - Under-replicated partitions
  - Consumer lag spikes
  - Stuck offsets
  - Broker imbalance
  - Disk saturation

**Hands-on Lab:**  
Diagnose and resolve simulated cluster degradation scenarios.

---

# Module 9 — Modern Kafka & Streaming Trends

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

---

# Module 10 — Capstone & Best Practices

- Architecture review of a real-time enterprise streaming use case
- Design checklist
  - Security
  - Scalability
  - Reliability
  - Observability
  - Governance
- End-to-end streaming architecture workshop
- Best practices review
- Q&A and technical recap

---
