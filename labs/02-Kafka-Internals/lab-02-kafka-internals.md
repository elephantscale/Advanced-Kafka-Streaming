# Lab 2 — Examining Kafka Internals

- **Module:** 2 — Kafka Internals & Cluster Architecture
- **Duration:** 60–75 minutes
- **Difficulty:** Intermediate
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

---

## Objectives

By the end of this lab you will be able to:

- Inspect raw log segment files on broker disk
- Read from the `__consumer_offsets` internal topic
- Trace a transactional producer commit sequence via `__transaction_state`
- Observe ISR changes during a simulated broker failure
- Use the idempotent producer and verify deduplication behavior
- Examine the KRaft metadata log

---

## Prerequisites

- Lab 1 completed (running 3-broker cluster)
- Python 3.9+ with `confluent-kafka` installed
- Java 17 (for Kafka CLI tools)

---

## Lab Environment

> **Lab environment** — same across all seven labs
>
> - Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free.
> - Local **Docker Compose** cluster. The main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
> - Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster. If a step reports one unavailable, treat it as instructor-led.
> - Full setup and prerequisites: `labs/SETUP.md`.

Same Docker Compose cluster from Lab 1.

```bash
# Verify cluster is running
docker compose ps

# Set broker shell alias
alias k1='docker exec kafka-1'
```

---

## Exercise 1 — Log Segments on Disk

### 1.1 Create a test topic and produce events

```bash
k1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic internals-test \
  --partitions 1 --replication-factor 1

# Produce 50 events
for i in $(seq 1 50); do
  echo "key-$((i%5)):value-$i"
done | k1 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic internals-test \
  --property parse.key=true \
  --property key.separator=:
```

### 1.2 Inspect log files on disk

```bash
# Find the log directory
docker exec kafka-1 ls -la /var/lib/kafka/data/internals-test-0/
```

Expected files:
```
00000000000000000000.log        ← event data
00000000000000000000.index      ← offset → byte position
00000000000000000000.timeindex  ← timestamp → offset
leader-epoch-checkpoint
```

### 1.3 Dump the log using DumpLogSegments

```bash
docker exec kafka-1 kafka-dump-log.sh \
  --files /var/lib/kafka/data/internals-test-0/00000000000000000000.log \
  --print-data-log \
  | head -40
```

**Questions:**

1. What fields are in each log entry?
2. Can you see the key and value in plaintext?
3. What is the `magic` field?

---

## Exercise 2 — Reading `__consumer_offsets`

### 2.1 Create a consumer group and consume some events

```bash
# Consume 10 events with a named group
k1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic internals-test \
  --group lab2-group \
  --max-messages 10 \
  --from-beginning
```

### 2.2 Read the `__consumer_offsets` topic

> Kafka 4 moved this formatter to the `org.apache.kafka.tools.consumer` package.
> The old `kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter`
> class was removed in 4.0 and will throw `ClassNotFoundException`.
> `--timeout-ms` stops the consumer once the topic is drained (it never ends on its own).

```bash
k1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic __consumer_offsets \
  --formatter org.apache.kafka.tools.consumer.OffsetsMessageFormatter \
  --from-beginning \
  --timeout-ms 10000 \
  2>/dev/null | grep lab2-group
```

### 2.3 Check offset via consumer-groups tool

```bash
k1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group lab2-group
```

**Questions:**

1. What information is stored in `__consumer_offsets`?
2. Where is the committed offset for partition 0 of `internals-test`?
3. Consume 10 more messages — how does the offset change?

---

## Exercise 3 — Transactional Producer

### 3.1 Write a transactional producer

Save as `transactional_producer.py`:

```python
from confluent_kafka import Producer
import json, time

conf = {
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'lab2-txn-producer',
    'enable.idempotence': True,
}

producer = Producer(conf)
producer.init_transactions()

def produce_transaction(order_ids):
    """Produce multiple events atomically."""
    producer.begin_transaction()
    try:
        for order_id in order_ids:
            event = {'order_id': order_id, 'status': 'CONFIRMED', 'ts': time.time()}
            producer.produce(
                topic='orders',
                key=order_id,
                value=json.dumps(event).encode()
            )
            print(f"  Queued: {order_id}")

        producer.commit_transaction()
        print(f"Transaction committed: {order_ids}")
    except Exception as e:
        print(f"Aborting transaction: {e}")
        producer.abort_transaction()

# Transaction 1: two orders atomically
produce_transaction(['txn-order-1', 'txn-order-2'])

# Transaction 2: another batch
produce_transaction(['txn-order-3', 'txn-order-4'])

print("Done.")
```

```bash
python transactional_producer.py
```

### 3.2 Consume with read_committed isolation

```python
# save as txn_consumer.py
from confluent_kafka import Consumer
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'txn-consumer-group',
    'auto.offset.reset': 'earliest',
    'isolation.level': 'read_committed',  # only see committed transactions
}

consumer = Consumer(conf)
consumer.subscribe(['orders'])

print("Consuming (read_committed)...")
for _ in range(20):
    msg = consumer.poll(timeout=2.0)
    if msg and not msg.error():
        data = json.loads(msg.value())
        print(f"  Partition {msg.partition()} offset {msg.offset()}: {data['order_id']}")
consumer.close()
```

```bash
python txn_consumer.py
```

**Questions:**

1. Are the transactional events visible to `read_committed` consumers?
2. What would happen if you used `read_uncommitted` isolation?
3. What happens to events from an aborted transaction?

### 3.3 Observe `__transaction_state`

> Kafka 4: formatter relocated to `org.apache.kafka.tools.consumer` (old Scala class removed).

```bash
k1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic __transaction_state \
  --formatter org.apache.kafka.tools.consumer.TransactionLogMessageFormatter \
  --from-beginning \
  --timeout-ms 10000 \
  2>/dev/null | head -20
```

**Questions:**

1. What states does the transaction go through?
2. What is the `producerId` (PID)?

---

## Exercise 4 — Idempotent Producer (Deduplication)

### 4.1 Simulate a duplicate send

```python
# save as idempotent_producer.py
from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'acks': 'all',
}

producer = Producer(conf)

# Simulate retrying the same event (as might happen on network error)
event = {'event_id': 'dedup-test-1', 'data': 'test'}

print("Sending event 3 times (simulating retries)...")
for attempt in range(3):
    producer.produce(
        topic='internals-test',
        key='dedup-key',
        value=json.dumps(event).encode(),
    )
    producer.flush()
    print(f"  Sent attempt {attempt + 1}")

print("Done. Check consumer — should see event only once (idempotent)")
```

```bash
python idempotent_producer.py

# Consume and count occurrences of dedup-test-1
k1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic internals-test \
  --from-beginning \
  --property print.key=true | grep dedup-key
```

**Note:** True deduplication requires the same `producer_id + sequence_number`. In this simulation, `flush()` between sends means each is a new request. True dedup happens on network-level retries within the same producer session.

### 4.2 See the PID and sequence numbers on disk

The idempotence metadata is written into the record batch headers. Dump the log
to see the `producerId`, `producerEpoch`, and `baseSequence` Kafka uses to dedup:

```bash
k1 kafka-dump-log.sh \
  --files /var/lib/kafka/data/internals-test-0/00000000000000000000.log \
  --print-data-log \
  | grep -E 'producerId|baseSequence' | tail -5
```

**Questions:**

1. What `producerId` (PID) was assigned to our producer? (Read it from the dump above — `-1` means a non-idempotent batch.)
2. How does Kafka use `producerId + producerEpoch + baseSequence` to detect duplicates?
3. How does the sequence number increment across the three sends?

---

## Exercise 5 — ISR Changes During Broker Failure

### 5.1 Observe current ISR

```bash
k1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders
```

Note the current ISR for each partition.

### 5.2 Stop a broker and watch ISR shrink

```bash
# In a separate terminal, watch ISR continuously
watch -n 2 "docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic orders 2>/dev/null | grep 'Isr:'"

# In another terminal, stop broker 2
docker compose stop kafka-2
```

Observe: partitions where broker-2 was in ISR will show ISR shrinking after `replica.lag.time.max.ms` (default 30 seconds).

### 5.3 Produce while a broker is down

First, enforce `min.insync.replicas=2` **on the topic** — this is a topic/broker
config, *not* a producer client setting. (Passing it to the producer would throw
`No such configuration property: "min.insync.replicas"`.) The producer's only job
is to request `acks=all`; the broker then refuses the write if fewer than 2
replicas are in sync.

```bash
k1 kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config min.insync.replicas=2
```

```python
# producer_test.py
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',                 # broker enforces min.insync.replicas (set on the topic)
    'request.timeout.ms': 5000,
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f"FAILED: {err}")
    else:
        print(f"OK: partition={msg.partition()}, offset={msg.offset()}")

producer.produce('orders', key='test', value='hello', callback=delivery_report)
producer.flush()
```

**Questions:**

1. Can you produce with `acks=all` when a broker is down (with ISR < `min.insync.replicas`)?
2. What error do you get?
3. Restart broker-2 — how long until ISR is restored?

```bash
docker compose start kafka-2
```

### 5.4 Observe Eligible Leader Replicas (Kafka 4, KIP-966)

Kafka 4 tracks an **ELR** — replicas that have dropped out of the ISR but are
known to still hold data up to the high watermark, so they can be elected leader
without data loss. `--describe` exposes it once the feature is active:

```bash
k1 kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders | grep -E 'Elr|Isr'
```

**Questions:**

1. When broker-2 left the ISR above, did its replicas appear in the `Elr` column?
2. How does ELR change the safety of a clean leader election when the ISR is empty?
3. What problem ("last replica standing") did Kafka have *before* ELR existed?

> If the `Elr` column is absent, the cluster's `metadata.version` predates ELR
> activation — note this as an environment check for your instructor.

---

## Exercise 6 — KRaft Metadata Log

```bash
# View KRaft metadata log
docker exec kafka-1 kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status

docker exec kafka-1 kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --replication
```

```bash
# Dump the metadata log
docker exec kafka-1 kafka-metadata-shell.sh \
  --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.checkpoint
```

**Questions:**

1. What is stored in the metadata log?
2. Which node is the active controller?
3. How does KRaft differ from ZooKeeper for metadata storage?

---

## Lab Summary

You have explored:

- Raw log segment files: `.log`, `.index`, `.timeindex`
- `__consumer_offsets`: how consumer positions are durably stored
- `__transaction_state`: how transactional producers track commit state
- Idempotent producer: duplicate prevention via PID + sequence number (read off disk)
- ISR dynamics: how ISR shrinks during broker failures and recovers on restart
- Eligible Leader Replicas (KIP-966): safe leader election beyond the ISR in Kafka 4
- KRaft metadata log: cluster metadata stored inside Kafka itself

**Key takeaway:** Kafka's reliability guarantees are built on a few well-designed internal mechanisms — append-only logs, ISR replication, and atomic transactions. Understanding these helps you tune and debug production issues with confidence.

---

## Review Questions

1. What three files make up a Kafka log segment, and what does each contain?
2. Under what conditions is a replica removed from the ISR?
3. What is the difference between `acks=1` and `acks=all`?
4. What does `min.insync.replicas=2` mean, on which entity is it configured, and when does it matter?
5. How do Eligible Leader Replicas (KIP-966) make a clean leader election safe when the ISR has emptied?

---

## What's Next

**Module 3** covers Kafka operations and observability — metrics, monitoring tools, operational runbooks, and incident triage for production clusters.

