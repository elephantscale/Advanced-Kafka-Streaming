   # Lab 1 — Exploring Kafka Cluster Topology and Topic Configuration

- **Module:** 1 — Modern Event-Driven Architecture with Kafka
- **Duration:** 60–75 minutes
- **Difficulty:** Foundational (for experienced developers new to Kafka)
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

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
Docker Compose stack (KRaft — no ZooKeeper):
  kafka-1  (combined broker+controller, port 9092)
  kafka-2  (combined broker+controller, port 9093)
  kafka-3  (combined broker+controller, port 9094)
  kafka-ui (web console, port 8080)

Your machine → localhost:9092 (mapped to kafka-1)
```

> **Lab environment (same across all seven labs):** Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free. These labs use a local **Docker Compose** cluster; the main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`. Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster; if a step reports one unavailable, treat it as instructor-led. Full setup and prerequisites: `labs/SETUP.md`.

Start the environment (run from the repository root, or any folder inside it —
`docker compose` searches parent directories for `docker-compose.yml`):
```bash
docker compose up -d
docker compose ps   # verify all containers are running
```

---

## Exercise 1 — Inspect Cluster Metadata

### 1.1 List brokers

```bash
docker exec kafka-1 kafka-cluster.sh cluster-id \
  --bootstrap-server localhost:9092

docker exec kafka-1 kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 \
  2>&1 | grep -E "^[a-z0-9.-]+:9092"
```

### 1.2 Describe the KRaft metadata quorum

In KRaft mode, cluster metadata is managed by a controller quorum (no ZooKeeper).
Inspect the quorum directly:

```bash
docker exec kafka-1 kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

**Questions:**
1. How many brokers are in the cluster?
2. Which node is the active controller (the quorum **leader**)?
3. What is the `LeaderEpoch`, and what does it tell you about controller stability?

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
# segment.ms + low dirty ratio force compaction to actually run quickly
# in the lab window (by default it would only act on rolled, non-active segments).
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic user-profiles \
  --partitions 6 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=10000 \
  --config min.compaction.lag.ms=0 \
  --config delete.retention.ms=100

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
  --timeout-ms 10000 \
  --property print.key=true \
  --property key.separator=" → "
```

> `--timeout-ms 10000` makes the consumer exit after 10s of no new messages, so you are not left at a blocking prompt. It will print a `TimeoutException` on exit — that is expected.

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

> **Kafka 4 note:** Kafka 4 introduces the new **server-side rebalance protocol (KIP-848)**. Instead of a stop-the-world rebalance where every consumer pauses while the group re-syncs, the broker coordinates incremental reassignment so unaffected consumers keep processing. You can check which protocol a group uses with:
>
> ```bash
> docker exec kafka-1 kafka-consumer-groups.sh \
>   --bootstrap-server localhost:9092 \
>   --describe --group payment-service --state
> ```

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

# Compaction only acts on a CLOSED (rolled) segment, not the active one.
# segment.ms=10000 rolls the segment after ~10s; the log cleaner then runs.
# Wait for the roll + a cleaner pass before reading back.
echo "Waiting ~30s for segment roll and log compaction..."
sleep 30

# After compaction, only the latest value for user-1 should remain
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-profiles \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.key=true
```

**Questions:**
1. After compaction, how many records exist for `user-1`? (You should see one — `v4`.)
2. What value does `user-1` have after compaction?
3. What guarantees the **latest** value per key always survives compaction?
4. Why does compaction never touch the active segment, and what config rolled it here?

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

## Exercise 7 — Visualize the Cluster in Kafka UI

So far you have inspected the cluster from the command line. Now see the same
structures graphically — this builds intuition fast.

### 7.1 Open Kafka UI

Browse to **http://localhost:8080**.

### 7.2 Explore

Click through and locate the following:

1. **Brokers** — confirm 3 brokers and which one is the active controller.
2. **Topics → `orders`** — view the 6 partitions, their leaders, replicas, and ISR.
   Compare against what `kafka-topics.sh --describe` showed you.
3. **Topics → `orders` → Messages** — browse the actual events; filter by key.
4. **Consumers → `payment-service`** — view per-partition lag as a live chart.

**Questions:**
1. Does the leader distribution shown in the UI match your CLI output from Exercise 6?
2. Where does the UI surface consumer lag, and would you spot a stuck consumer faster here or via the CLI?
3. Which view would you hand to an on-call engineer during an incident, and why?

---

## Exercise 8 — Streams vs Queues: Share Groups (Kafka 4)

> **Preview feature / instructor-led.** Share Groups (KIP-932) are *early access*
> in Kafka 4.0 and must be explicitly enabled on the cluster (the lab cluster is
> pre-configured with `group.share.enable=true` and the share-group coordinator).
> If your environment does not have it enabled, treat this as a demonstration.

In Exercise 4, a **consumer group** split the partitions across consumers — each
partition was owned by exactly one consumer. A **share group** is Kafka's native
**queue**: many consumers pull from the *same* partitions cooperatively, and each
record is acknowledged individually.

### 8.1 Start two share consumers on the same topic

Open **terminal 1:**
```bash
docker exec kafka-1 kafka-console-share-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-workers \
  --property print.partition=true
```

Open **terminal 2** (same share group, same topic):
```bash
docker exec kafka-1 kafka-console-share-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-workers \
  --property print.partition=true
```

### 8.2 Produce work and watch distribution

```bash
for i in $(seq 1 20); do
  echo "job-$i:{\"job\":$i}" | docker exec -i kafka-1 kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --property key.separator=: --property parse.key=true
done
```

### 8.3 Inspect the share group

```bash
docker exec kafka-1 kafka-share-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group order-workers
```

**Questions:**
1. In Exercise 4 each partition went to exactly one consumer. How is record
   delivery distributed across the two **share** consumers here?
2. Why does a share group not need more consumers than partitions to scale
   (unlike a classic consumer group)?
3. Name one workload where queue semantics (share group) fit better than a
   consumer group, and one where the reverse is true.

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

- KRaft cluster structure: brokers, the controller quorum, topics, partitions, replicas, ISR
- Topic creation with different partition counts, replication factors, and policies
- Produce and consume events using the CLI
- Consumer group partition assignment and rebalancing (incl. the Kafka 4 KIP-848 protocol)
- Retention policies: time-based deletion and log compaction (observed end to end)
- Broker failure impact on partition leadership
- Visualizing the cluster in Kafka UI
- Native queue semantics with Share Groups (KIP-932) vs consumer groups

**Key takeaway:** Kafka's durability comes from replication; its scalability comes from partitioning. Understanding both is the foundation for everything else in this course.

---

## Review Questions

1. What is the relationship between partition count and maximum consumer parallelism?
2. What happens to partition leaders when a broker fails?
3. What is the difference between a `deletion` and a `compact` retention policy?
4. If a consumer group has 4 consumers and a topic has 6 partitions, how are partitions distributed?
5. How does a **share group** (queue semantics) differ from a **consumer group** in how records map to consumers?
6. What problem does the KIP-848 rebalance protocol solve compared to the older client-side protocol?

---

## What's Next

**Module 2** goes deep inside the Kafka broker — log segments, KRaft, ISR mechanics, upgrade strategies, and broker configuration best practices.

