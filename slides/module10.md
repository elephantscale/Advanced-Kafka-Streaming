# Module 10 — Capstone & Best Practices

Elephant Scale

---

## Module 10 Agenda

- Architecture review of a real-time enterprise streaming use case
- End-to-end streaming architecture workshop
- Design checklist: security, scalability, reliability, observability, governance
- Best practices review
- Q&A and technical recap

---

## The Capstone Challenge

**Scenario:** You are the streaming platform architect at a global e-commerce company.

Requirements:
- Process 500,000 orders/day with peak spikes of 50,000 orders/hour
- Support fraud detection with < 200ms latency
- Integrate with 5 databases, S3 data lake, and Elasticsearch
- Operate across 3 cloud regions (us-east-1, eu-west-1, ap-southeast-1)
- Comply with GDPR (EU data must stay in EU) and PCI DSS
- 99.99% uptime SLA

> How would you design this platform?

---

## Architecture Review — Context

```
Data Sources:
  Order Service (PostgreSQL)
  Payment Service (PostgreSQL)
  Inventory Service (MongoDB)
  User Service (MySQL)
  Web/Mobile Events (HTTP)

Data Consumers:
  Fraud Detection (real-time ML)
  Recommendations Engine
  Data Warehouse (Redshift)
  Operational Dashboards (Elasticsearch)
  Regulatory Reporting (S3)
```

---

## Architecture Review — Kafka Topology

```
┌─────────────────────────────────────────┐
│  Region: us-east-1                       │
│  Kafka Cluster (3 AZs, 9 brokers)        │
│                                          │
│  Topics:                                 │
│    prod.orders.order.placed (24 parts)   │
│    prod.payments.payment.completed       │
│    prod.inventory.stock.updated          │
│    prod.users.user.updated (compacted)   │
│    prod.events.web.clickstream           │
└────────────────┬────────────────────────┘
                 │  MirrorMaker 2
                 ▼
┌────────────────────────────────┐
│  Region: eu-west-1              │
│  (GDPR: EU data stays local)    │
│  prod.eu.orders.order.placed    │
└────────────────────────────────┘
```

---

## Architecture Review — Connectors

```
Sources:
  Debezium (PostgreSQL CDC) → order, payment events
  Debezium (MySQL CDC)     → user events
  MongoDB Source Connector → inventory events
  HTTP Source Connector    → web/mobile clickstream

Sinks:
  S3 Sink (Parquet)        → data lake for all topics
  Elasticsearch Sink       → ops.orders, ops.errors
  Redshift Sink            → data warehouse topics
  JDBC Sink                → reporting database
```

---

## Architecture Review — Stream Processing

```
Kafka Streams Applications:

1. fraud-detector
   Input:  prod.orders.order.placed
   Joins:  user-profile KTable, payment-history KTable
   Window: 5-minute sliding window
   Output: fraud-scores, flagged-orders

2. order-enricher
   Input:  prod.orders.order.placed
   Joins:  inventory KTable, user KTable
   Output: prod.orders.order.enriched

3. metrics-aggregator (ksqlDB)
   Input:  prod.orders.order.*
   Output: hourly-revenue, daily-conversion-rates
```

---

## Architecture Review — Security Design

```
Authentication: SASL/SCRAM-SHA-512 (all clusters)
  → Service accounts per microservice
  → Credentials in HashiCorp Vault (auto-rotation)

Authorization: ACLs (least privilege)
  → order-service: Write to prod.orders.*
  → fraud-detector: Read prod.orders.*, Write fraud-scores
  → data-engineer: Read *, no Write

Encryption:
  → TLS 1.3 on all listeners
  → PII fields (name, email, card) encrypted at source
  → EU topics: separate encryption key (GDPR)

Compliance:
  → EU topics replicated only within EU cluster
  → PAN (card numbers) masked before Kafka
  → Audit log: all ACL denies logged to SIEM
```

---

## Architecture Review — Observability

```
Metrics pipeline:
  JMX Exporter → Prometheus → Grafana

Dashboards:
  - Cluster Overview (URPs, controller, broker health)
  - Topic Throughput (bytes/sec, message rate)
  - Consumer Lag (per group, per partition) via Burrow
  - Fraud Detection Pipeline (latency, throughput, accuracy)
  - Connector Health (task status, error rates)

Alerts (PagerDuty):
  - URPs > 0 for > 2 min
  - Fraud detector lag > 10,000 messages
  - Connector task FAILED
  - Broker disk > 85%

SLO: 99.99% uptime → < 52 min downtime/year
```

---

## Design Checklist — Security

- [ ] TLS 1.2+ on all listeners (no plaintext in production)
- [ ] SASL authentication for all clients (no anonymous access)
- [ ] ACLs or RBAC — least privilege per service
- [ ] No wildcard ACLs (`*`) in production
- [ ] Secrets in vault — no passwords in config files or environment variables
- [ ] PII masked or encrypted before producing
- [ ] Audit logging enabled
- [ ] Network isolation — Kafka not reachable from public internet
- [ ] Regular credential rotation
- [ ] Schema validation enforced (no raw JSON in production)

---

## Design Checklist — Scalability

- [ ] Partition count sized for target throughput
- [ ] Partition key chosen to avoid hot partitions
- [ ] Replication factor = 3 for all production topics
- [ ] `min.insync.replicas = 2` for all production topics
- [ ] Consumer group per service (not shared)
- [ ] Cooperative sticky rebalancing enabled
- [ ] Cruise Control for automated rebalancing
- [ ] Tiered storage for large topics with long retention
- [ ] Capacity plan: storage, network, CPU per broker
- [ ] Load test before go-live

---

## Design Checklist — Reliability

- [ ] `acks=all` on all production producers
- [ ] `enable.idempotence=true` on all producers
- [ ] Transactional producers for exactly-once pipelines
- [ ] `isolation.level=read_committed` on EOS consumers
- [ ] Dead letter queue for all connectors
- [ ] Retry with exponential backoff in all consumers
- [ ] Health checks for all Kafka Streams apps
- [ ] Standby replicas for stateful Kafka Streams apps
- [ ] DR cluster configured with MirrorMaker 2
- [ ] Failover playbook documented and tested

---

## Design Checklist — Observability

- [ ] Prometheus + Grafana deployed and scraping all brokers
- [ ] Burrow deployed for consumer lag monitoring
- [ ] Alerts configured for: URPs, controller, lag, disk, connector failures
- [ ] Runbooks for each alert
- [ ] Distributed tracing headers propagated through events
- [ ] End-to-end latency measured (produce timestamp → consume timestamp)
- [ ] Log aggregation for broker, connect, streams logs
- [ ] SLO dashboards visible to stakeholders

---

## Design Checklist — Governance

- [ ] Topic naming convention documented and enforced
- [ ] Every topic has a declared owner
- [ ] Schema Registry with all production schemas registered
- [ ] Schema compatibility policy set (BACKWARD or FULL)
- [ ] Retention policies set explicitly on all topics
- [ ] Topic lifecycle process: creation, deprecation, deletion
- [ ] Data catalog entries for all production topics
- [ ] PII fields documented in schema metadata
- [ ] Compliance topics identified (GDPR, PCI, HIPAA)
- [ ] Data lineage tracked

---

## Best Practices — Producers

1. Always use `acks=all` + `enable.idempotence=true`
2. Set `batch.size` and `linger.ms` for throughput
3. Use compression (`lz4` or `zstd`)
4. Never swallow send exceptions — handle `Future.get()` errors
5. Use a `transactional.id` for exactly-once pipelines
6. Include event metadata in every record: timestamp, event_type, source
7. Register schema before first production use

---

## Best Practices — Consumers

1. Always use manual offset commit in production
2. Commit after processing (at-least-once) or use transactions
3. Set `group.instance.id` to avoid rebalance on restart
4. Use `CooperativeStickyAssignor` for minimal rebalance disruption
5. Tune `max.poll.interval.ms` to match your slowest processing step
6. Always handle `WakeupException` and `RebalanceListener`
7. Send unprocessable messages to DLQ — never block the consumer

---

## Best Practices — Operations

1. Use Cruise Control for partition rebalancing (not manual reassignment)
2. Test DR failover quarterly — not just after incidents
3. Monitor consumer lag continuously — set SLA-based alert thresholds
4. Use rolling upgrades, one broker at a time
5. Never reduce replication factor without a maintenance window
6. Keep retention short during incidents; restore after resolution
7. Document every manual operation in a change log

---

## Common Anti-Patterns

 Anti-Pattern  Better Approach
---------
 One topic for everything  Separate topics per event type
 Unkeyed producers for ordered data  Use partition keys consistently
 Auto-commit with complex processing  Manual commit after processing
 Large messages (MB+) in Kafka  Store large payloads in S3, put URL in event
 Single partition topics  At least match consumer instance count
 Monitoring only broker metrics  Monitor consumer lag and pipeline latency
 Shared consumer group across services  One consumer group per service
 No schema on topics  Register schemas in Schema Registry

---

## Workshop — End-to-End Streaming Design

**Exercise (30 minutes):**

Design a streaming platform for a ride-sharing company:
- Drivers report GPS location every 5 seconds (1M drivers)
- Riders request trips (10K requests/minute peak)
- Match driver to rider in real time (< 500ms)
- Surge pricing: update per zone every 30 seconds
- Driver earnings: aggregate per driver per day

**Deliverables:**
1. Topic design (names, partitions, retention, compaction)
2. Consumer group topology
3. Stream processing logic for matching and surge pricing
4. Security design (auth, ACLs, PII handling)
5. Observability strategy

---

## Kafka Ecosystem — Quick Reference

 Tool  Purpose  When to Use
--------
 MirrorMaker 2  Cross-cluster replication  Multi-region, DR
 Kafka Streams  Stream processing (JVM)  Complex stateful processing
 ksqlDB  SQL on streams  Analysts, simple transformations
 Kafka Connect  Data integration  Ingest/egress to external systems
 Schema Registry  Schema management  All production topics
 Cruise Control  Cluster operations  Rebalancing, anomaly detection
 Burrow  Consumer lag monitoring  SLA alerting on lag
 Strimzi  Kafka on Kubernetes  Cloud-native deployments

---

## Module 10 Summary

- Applied a full architecture review across all 9 course modules
- End-to-end design considers: sources, processing, sinks, security, observability
- Checklists turn knowledge into actionable production standards
- Common anti-patterns: unkeyed producers, shared consumer groups, no schemas
- The streaming platform is not just Kafka — it's the whole ecosystem working together

---

## Course Summary

You have mastered:

| Module | Key Skill |
|--------|-----------|
| 1 | Event-driven architecture patterns |
| 2 | Kafka internals: logs, ISR, KRaft, transactions |
| 3 | Topic design, schema evolution, cross-cluster replication |
| 4 | Stream processing: Kafka Streams, ksqlDB, windowing |
| 5 | Kafka Connect: connectors, error handling, integrations |
| 6 | Performance tuning, rebalancing, capacity planning |
| 7 | Security: TLS, SASL, ACLs, governance |
| 8 | Observability: metrics, alerting, troubleshooting |
| 9 | Modern Kafka: edge, AI, serverless, queues |
| 10 | Architecture design and best practices |

---

## Thank You

**© Elephant Scale**

Questions? Continue the conversation:
- Course materials: provided repository
- Kafka documentation: kafka.apache.org
- Confluent documentation: docs.confluent.io
- Kafka community Slack: launchpass.com/confluentcommunity

---

