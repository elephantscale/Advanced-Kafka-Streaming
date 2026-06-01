# Lab 1 — Exploring Kafka Cluster Topology and Topic Configuration

**Module:** 1 — Modern Event-Driven Architecture with Kafka
**Duration:** 45–60 minutes
**Difficulty:** Beginner

---

## Objectives

By the end of this lab you will be able to:

- Connect to a running Kafka cluster and inspect broker metadata
- Create topics with different partition counts and replication factors
- Examine partition assignment and leader distribution
- Configure and observe retention policies
- Produce and consume events using command-line tools
- Understand how consumer groups work in practice

---

## Prerequisites

- Docker and Docker Compose installed
- `kafkacat` / `kcat` or Kafka CLI tools available
- Lab Docker Compose file (provided)

---

## Lab Environment

```
Docker Compose stack:
  kafka-1  (broker 1, port 9092)
  kafka-2  (broker 2, port 9093)
  kafka-3  (broker 3, port 9094)
  zookeeper (or KRaft — see instructor)

Your machine → localhost:9092 (mapped to kafka-1)
```

Start the environment:
```bash
docker compose up -d
docker compose ps   # verify all containers are running
```

Set a convenience alias:
```bash
alias kafka='docker exec -it kafka-1 /bin/bash -c'
```

---

## Exercise 1 — Inspect Cluster Metadata

### 1.1 List brokers

```bash
docker exec kafka-1 kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 \
  2>&1 | grep "id:"
```

### 1.2 Describe the cluster

```bash
docker exec kafka-1 kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  --command-config /dev/null \
  describe --status
```

**Questions:**
1. How many brokers are in the cluster?
2. Which broker is the active controller?
3. What Kafka version is running?

---

## Exercise 2 — Create Topics

### 2.1 Create a basic topic

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 6 \
  --replication-factor 3
```

### 2.2 Describe the topic

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

Example output:
```
Topic: orders  Partitions: 6  ReplicationFactor: 3
  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  ...
```

**Questions:**
1. How are leaders distributed across brokers?
2. What does the ISR column mean?
3. Are all replicas in the ISR?

### 2.3 Create topics with different configurations

```bash
# High-throughput topic (many partitions)
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic clickstream \
  --partitions 24 --replication-factor 3

# Compacted topic (for state)
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic user-profiles \
  --partitions 6 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5

# Short retention (for transient data)
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic temp-events \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=3600000  # 1 hour
```

---

## Exercise 3 — Produce and Consume Events

### 3.1 Produce events

```bash
# Simple producer (key:value format)
docker exec -it kafka-1 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property key.separator=: \
  --property parse.key=true
```

Type these events (press Enter after each):
```
order-1:{"id":"order-1","customer":"alice","amount":99.99}
order-2:{"id":"order-2","customer":"bob","amount":249.50}
order-1:{"id":"order-1","customer":"alice","amount":109.99}
order-3:{"id":"order-3","customer":"carol","amount":19.99}
```

Press Ctrl+C to exit.

### 3.2 Consume from the beginning

```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" → "
```

**Questions:**
1. Are events for `order-1` in the same partition?
2. What is the order of events for the same key?
3. What happens if you consume without `--from-beginning`?

---

## Exercise 4 — Consumer Groups

### 4.1 Start two consumers in the same group

Open **terminal 1:**
```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group payment-service \
  --from-beginning \
  --property print.partition=true
```

Open **terminal 2** (same group, same topic):
```bash
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group payment-service \
  --property print.partition=true
```

### 4.2 Describe the consumer group

```bash
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group payment-service
```

**Questions:**
1. How are partitions divided between the two consumers?
2. What is the current lag for each partition?
3. Start a third consumer in the same group — how do partitions rebalance?

---

## Exercise 5 — Retention Policies

### 5.1 Observe retention behavior

```bash
# Produce 100 events
for i in $(seq 1 100); do
  echo "key-$((i % 10)):event-$i" | docker exec -i kafka-1 kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --property key.separator=: \
    --property parse.key=true
done
```

### 5.2 Alter retention on a live topic

```bash
# Reduce retention to 10 minutes
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name orders \
  --add-config retention.ms=600000

# Verify
docker exec kafka-1 kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --entity-type topics \
  --entity-name orders
```

### 5.3 Observe compaction on user-profiles

```bash
# Produce state updates for 3 users
for i in 1 2 3; do
  echo "user-$i:{\"name\":\"User $i v1\"}" | docker exec -i kafka-1 \
    kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic user-profiles \
    --property key.separator=: --property parse.key=true
done

# Update user-1 multiple times
for v in 2 3 4; do
  echo "user-1:{\"name\":\"User 1 v$v\"}" | docker exec -i kafka-1 \
    kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic user-profiles \
    --property key.separator=: --property parse.key=true
done

# After compaction runs, only user-1 v4 should remain for user-1
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-profiles \
  --from-beginning \
  --property print.key=true
```

**Questions:**
1. After compaction, how many records exist for `user-1`?
2. What value does `user-1` have after compaction?
3. When does compaction actually run?

---

## Exercise 6 — Partition Layout and Leader Distribution

### 6.1 Check leader distribution

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders | grep Leader | awk '{print $4}' | sort | uniq -c
```

### 6.2 Simulate a broker failure

```bash
# Stop broker 3
docker compose stop kafka-3

# Observe partition reassignment
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

**Questions:**
1. Which partitions changed their leader?
2. Are any partitions under-replicated now?
3. Start broker-3 again — do leaders automatically rebalance?

```bash
docker compose start kafka-3
# Wait ~30 seconds, then:
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic orders
```

---

## Challenge Exercise (Optional)

Write a Python script using the `confluent-kafka` library that:

1. Creates a topic programmatically (using AdminClient)
2. Produces 1,000 events with random keys (10 distinct keys)
3. Consumes all events and counts how many went to each partition
4. Reports the partition distribution

```python
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer
import random, json

# Your implementation here
```

**Questions:**
1. Is the partition distribution even?
2. What determines which partition each key goes to?
3. How many events per key per partition?

---

## Lab Summary

You have explored:

- Kafka cluster structure: brokers, topics, partitions, replicas, ISR
- Topic creation with different partition counts, replication factors, and policies
- Produce and consume events using the CLI
- Consumer group partition assignment and rebalancing
- Retention policies: time-based deletion and log compaction
- Broker failure impact on partition leadership

**Key takeaway:** Kafka's durability comes from replication; its scalability comes from partitioning. Understanding both is the foundation for everything else in this course.

---

## Review Questions

1. What is the relationship between partition count and maximum consumer parallelism?
2. What happens to partition leaders when a broker fails?
3. What is the difference between a `deletion` and a `compact` retention policy?
4. If a consumer group has 4 consumers and a topic has 6 partitions, how are partitions distributed?

---

## What's Next

**Module 2** goes deep inside the Kafka broker — log segments, KRaft, ISR mechanics, upgrade strategies, and broker configuration best practices.

