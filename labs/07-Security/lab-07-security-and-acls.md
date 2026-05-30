# Lab 7 — Kafka Security: TLS, SCRAM, and ACLs

**Module:** 7 — Security & Governance  
**Duration:** 60-75 minutes  
**Difficulty:** Intermediate

---

## Objectives

By the end of this lab, you will be able to:

- Enable authentication and encryption for Kafka clients
- Create SCRAM users and validate login behavior
- Apply least-privilege ACLs for producer and consumer principals
- Verify authorization failures for unauthorized operations
- Audit access decisions with CLI checks

---

## Prerequisites

- Running Kafka cluster from previous labs
- Docker Compose environment with Kafka CLI access
- `openssl` installed locally

---

## Lab Environment

This lab assumes a security-enabled listener is available on `SASL_SSL://localhost:9093`.
If your current compose profile is PLAINTEXT-only, use your instructor-provided secure profile.

```bash
# Example check for listener availability
nc -zv localhost 9093
```

Create a local folder for client properties:

```bash
mkdir -p ./security
```

---

## Exercise 1 — Create SCRAM Users

Create two service users:
- `svc_orders_producer`
- `svc_orders_consumer`

```bash
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=prod-secret-1]' \
  --entity-type users \
  --entity-name svc_orders_producer

docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=cons-secret-1]' \
  --entity-type users \
  --entity-name svc_orders_consumer
```

Create client property files.

`security/producer.properties`

```properties
bootstrap.servers=localhost:9093
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="svc_orders_producer" password="prod-secret-1";
ssl.truststore.location=/etc/kafka/secrets/client.truststore.jks
ssl.truststore.password=changeit
```

`security/consumer.properties`

```properties
bootstrap.servers=localhost:9093
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="svc_orders_consumer" password="cons-secret-1";
ssl.truststore.location=/etc/kafka/secrets/client.truststore.jks
ssl.truststore.password=changeit
```

---

## Exercise 2 — Create a Protected Topic

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --create \
  --topic prod.secure.orders \
  --partitions 6 \
  --replication-factor 3
```

Verify topic exists.

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --describe --topic prod.secure.orders
```

---

## Exercise 3 — Add Least-Privilege ACLs

Grant producer write/describe and consumer read/group.

```bash
# Producer permissions
docker exec kafka-1 kafka-acls.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:svc_orders_producer \
  --operation Write \
  --operation Describe \
  --topic prod.secure.orders

# Consumer permissions
docker exec kafka-1 kafka-acls.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:svc_orders_consumer \
  --operation Read \
  --operation Describe \
  --topic prod.secure.orders

docker exec kafka-1 kafka-acls.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:svc_orders_consumer \
  --operation Read \
  --group secure-orders-cg
```

List ACLs:

```bash
docker exec kafka-1 kafka-acls.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --list
```

---

## Exercise 4 — Positive Validation

Produce as `svc_orders_producer`:

```bash
docker exec -i kafka-1 kafka-console-producer.sh \
  --producer.config /workspace/security/producer.properties \
  --topic prod.secure.orders <<'EOF'
{"order_id":"s-1","amount":49.95}
{"order_id":"s-2","amount":89.10}
EOF
```

Consume as `svc_orders_consumer`:

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --consumer.config /workspace/security/consumer.properties \
  --topic prod.secure.orders \
  --group secure-orders-cg \
  --from-beginning \
  --max-messages 2
```

---

## Exercise 5 — Negative Validation (Authorization Failures)

Try to consume using producer credentials (should fail):

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --consumer.config /workspace/security/producer.properties \
  --topic prod.secure.orders \
  --group unauthorized-cg \
  --timeout-ms 8000
```

Try to produce using consumer credentials (should fail):

```bash
docker exec -i kafka-1 kafka-console-producer.sh \
  --producer.config /workspace/security/consumer.properties \
  --topic prod.secure.orders <<'EOF'
{"order_id":"should-fail","amount":1}
EOF
```

**Expected:** `TopicAuthorizationException` and/or `GroupAuthorizationException`.

---

## Exercise 6 — Governance Quick Checks

Run governance checks for one production topic:

```bash
# Topic retention policy
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --entity-type topics \
  --entity-name prod.secure.orders \
  --describe

# ACL audit for this topic
docker exec kafka-1 kafka-acls.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin.properties \
  --list \
  --topic prod.secure.orders
```

Document:
- Topic owner
- Retention rationale
- Allowed principals and operations

---

## Lab Summary

You implemented:

- SCRAM user authentication
- ACL-based least-privilege authorization
- Positive and negative access tests
- Governance checks for retention and ownership

**Key takeaway:** Security in Kafka is layered. Authentication confirms identity; ACLs enforce permissions; governance ensures controls stay consistent over time.

---

## Review Questions

1. Why must consumer group ACLs be granted separately from topic ACLs?
2. What is the risk of broad wildcard ACLs in production?
3. How would you rotate SCRAM credentials with minimal downtime?

---

## What's Next

**Module 8** focuses on observability and operations: metrics, alerts, and incident troubleshooting.

