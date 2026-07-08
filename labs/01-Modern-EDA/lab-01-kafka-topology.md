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

> **Lab environment** — same across all seven labs
>
> - Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free.
> - Local **Docker Compose** cluster. The main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
> - Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster. If a step reports one unavailable, treat it as instructor-led.
> - Full setup and prerequisites: `labs/SETUP.md`.
> - **Python exercises:** activate the virtualenv first — `source .venv/bin/activate` (created per `labs/SETUP.md`).

Start the environment (run from the repository root, or any folder inside it —
`docker compose` searches parent directories for `docker-compose.yml`):

```bash
docker compose up -d
docker compose ps   # verify all containers are running
```

---

## Exercise 1 — Inspect Cluster Metadata

> **What this shows:** You are reading cluster metadata straight from the source. In KRaft mode the cluster ID, broker registry, and topic/partition state live in an internal metadata log replicated by a controller quorum — there is no ZooKeeper. Expect to see three brokers and one active controller; `kafka-metadata-quorum.sh` exposes the Raft quorum itself (leader, voters, epoch), which is the authoritative view of cluster membership.
>
> **If a sharp student asks:** "Is the active controller also a broker?" In this lab the nodes are *combined* (`process.roles=broker,controller`), so yes — but in production you typically run dedicated controllers; the controller quorum is a separate Raft group from the data-plane brokers even when co-located.

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

> **What this shows:** Topics are just named, partitioned logs. Partitions are the unit of parallelism and ordering; replication factor is the unit of durability. Creating `orders` with 6 partitions and RF 3 means the data survives losing any one broker, and up to 6 consumers in a group can read it in parallel. Expect the leaders and replicas to be spread roughly evenly across the three brokers.
>
> **If a sharp student asks:** "Why can't I just crank partitions up to 1000 to be safe?" Every partition costs open file handles, memory, and replication/metadata overhead, and raises end-to-end latency and rebalance/recovery time; partition count is easy to raise later but impossible to lower, so you size for realistic parallelism, not the maximum.

### 2.1 Create a basic topic

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
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
  --create --if-not-exists --topic clickstream \
  --partitions 24 --replication-factor 3

# Compacted topic (for state).
# ONE partition so every key shares it, and a tiny segment.bytes so segments roll
# by SIZE as records arrive — compaction only cleans CLOSED segments, and size-based
# rolling makes that happen reliably in the lab window (no timing guesswork).
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic user-profiles \
  --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.bytes=512 \
  --config min.compaction.lag.ms=0 \
  --config delete.retention.ms=100

# Short retention (for transient data)
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic temp-events \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=3600000  # 1 hour
```

> **Why:** These aggressive compaction settings (`min.cleanable.dirty.ratio=0.01`, tiny `segment.bytes`, zero lag) exist only to force the log cleaner to act *inside the lab window*. Production defaults are far lazier — compaction is a background optimization, not a real-time guarantee — so never copy these numbers into a real cluster. Note also that a compacted topic uses a single partition here so every version of a given key lands in one log where the cleaner can collapse it.

---

## Exercise 3 — Produce and Consume Events

> **What this shows:** This is the core Kafka contract: a producer sends keyed records, the key's hash picks the partition, and order is guaranteed *only within a partition*. Because `order-1` appears twice with the same key, both copies land in the same partition and are consumed in produce order. `--from-beginning` reads the log from offset 0; without it, a fresh consumer only sees records produced after it joins.
>
> **If a sharp student asks:** "Does same-key-same-partition hold if I later add partitions?" No — the default hash partitioner maps `hash(key) % numPartitions`, so increasing partition count re-routes existing keys to different partitions. Historical ordering per key is only preserved for data already written; that is one reason partition count is treated as near-immutable.

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

> **What this shows:** A consumer group is Kafka's load-balancing and fault-tolerance mechanism. The group coordinator assigns each partition to exactly one consumer in the group, so with 6 partitions and 2 consumers each reads ~3. This is why parallelism caps at the partition count — a third consumer helps, a seventh would sit idle. `kafka-consumer-groups.sh --describe` shows the assignment, committed offsets, and lag (log-end offset minus committed offset).
>
> **If a sharp student asks:** "What actually happens during a rebalance when I add the third consumer?" The coordinator revokes and reassigns partitions to rebalance ownership. With the legacy protocol this is stop-the-world (all consumers pause); with KIP-848 (Kafka 4, referenced below) the broker drives incremental reassignment so unaffected consumers keep processing.

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

Leave the two consumers running in terminals 1 and 2. Open a **third terminal** for this command:

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

> **Before moving on:** stop the two consumers (Ctrl+C) and close terminals 1 and 2.
> They are no longer needed — and leaving them running will consume the events you
> produce in Exercise 5, muddying the retention test.

---

## Exercise 5 — Retention Policies

> **What this shows:** Kafka has two independent cleanup policies. `delete` (time/size-based) drops whole old segments; `compact` keeps the latest value per key and garbage-collects superseded ones. You alter retention on a live topic to prove config is dynamic, then watch compaction reduce `user-1`'s four versions down to just `v4`. Compaction is what makes a topic usable as a durable key-value changelog (the basis for Kafka Streams state stores).
>
> **If a sharp student asks:** "How do I *delete* a key under compaction, not just keep the latest?" You produce a tombstone — a record with that key and a `null` value. The cleaner retains it long enough for consumers to observe the delete (governed by `delete.retention.ms`, set very low here), then removes the key entirely.

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

<!-- Verified working on Kafka 4.0: after this runs, user-1 shows only v4. -->

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

# Compaction only cleans CLOSED segments, never the active one. Produce some filler
# records to the SAME (single) partition — each one pushes the log past segment.bytes
# and rolls a new segment, so the segment holding the user-1 updates closes and becomes
# compactable.
for n in 1 2 3 4 5 6 7 8; do
  echo "filler-$n:{\"pad\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}" | docker exec -i kafka-1 \
    kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic user-profiles \
    --property key.separator=: --property parse.key=true
done

# Give the log cleaner a pass to compact the closed segments.
echo "Waiting 60s for the log cleaner to compact..."
sleep 60

# After compaction, only the LATEST value for user-1 (v4) should remain
# (user-2, user-3 and the filler-* keys each appear once too — that's expected).
docker exec kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-profiles \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.key=true
```

> **Why the filler records?** This is the step students most often "fix" incorrectly. The cleaner never touches the *active* (open) segment — the one currently being appended. The filler records exist purely to push the log past `segment.bytes` and roll new segments, which *closes* the segment holding the `user-1` updates so it finally becomes eligible for compaction. Skip the filler and you'll wrongly conclude compaction is broken.

> **If you still see all versions of `user-1`:** compaction is a background process —
> wait another 30–60s and re-run the consumer. Confirm the topic config with
> `kafka-configs.sh --describe --entity-type topics --entity-name user-profiles`
> (it must show `cleanup.policy=compact`).

**Questions:**

1. After compaction, how many records exist for `user-1`? (You should see one — `v4`.)
2. What value does `user-1` have after compaction?
3. What guarantees the **latest** value per key always survives compaction?
4. Why does compaction never touch the active segment, and what config rolled it here?

---

## Exercise 6 — Partition Layout and Leader Distribution

> **What this shows:** Only the partition *leader* handles reads and writes; followers just replicate. Kafka spreads leadership evenly across brokers so load is balanced — the `uniq -c` count should show each broker leading roughly the same number of partitions. When you stop broker 3, the controller promotes an in-sync replica to leader for its partitions (brief unavailability, no data loss), and those partitions become under-replicated until the broker returns.
>
> **If a sharp student asks:** "When broker 3 comes back, why don't leaders rebalance immediately?" Each partition has a *preferred* leader (first replica in the assignment list). On restart the replica rejoins as a follower and catches up; leadership only returns on the next preferred-leader election — automatic if `auto.leader.rebalance.enable=true`, otherwise forced with `kafka-leader-election.sh`.

### 6.1 Check leader distribution

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders | grep Leader | awk '{print $6}' | sort | uniq -c
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

> **What this shows:** The UI reads the exact same cluster metadata you queried on the CLI — brokers, the active controller, partition leaders/replicas/ISR, message contents, and consumer lag — but renders it visually. The goal is to build a mental map linking each `kafka-*.sh --describe` field to what an operator sees on a dashboard. Lag shown as a live chart is the fastest way to spot a stuck or slow consumer.
>
> **If a sharp student asks:** "Is the UI authoritative, or could it disagree with the CLI?" It is a read-only client hitting the same broker APIs, so the data matches; any lag you see is just its polling/refresh interval. It holds no state of its own — deleting the UI container loses nothing.

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

> **What this shows:** Share groups (KIP-932) give Kafka true *queue* semantics. Unlike a consumer group — where one partition maps to one consumer — many share consumers cooperatively read the *same* partitions, and each record is acknowledged individually. Delivery is decoupled from partition count, so you can add more workers than partitions and still spread load. Expect the 20 jobs to fan out across both share consumers regardless of how few partitions `orders` has.
>
> **If a sharp student asks:** "So does a share group still guarantee per-key ordering like a consumer group?" No — that is the trade-off. Records are handed out per-record with individual acks (and redelivery on failure), so you gain queue-style work distribution and competing consumers but give up the strict per-partition ordering a consumer group provides.

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

Leave both share consumers running. Open a **third terminal** and produce 20 jobs — watch how they spread across the two consumers:

```bash
for i in $(seq 1 20); do
  echo "job-$i:{\"job\":$i}" | docker exec -i kafka-1 kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --property key.separator=: --property parse.key=true
done
```

### 8.3 Inspect the share group

In the third terminal (the produce loop has finished), inspect the share group:

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

> **Done with the hands-on section.** Stop any running consumers (Ctrl+C) and close all
> terminals. (The Challenge below is an optional Python exercise.)

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
