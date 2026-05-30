# Lab 6 — Stress-Testing a Kafka Cluster

**Module:** 6 — Reliability, Scaling & Performance
**Duration:** 60–75 minutes
**Difficulty:** Intermediate–Advanced

---

## Objectives

By the end of this lab you will be able to:

- Baseline benchmark producer and consumer throughput
- Tune batch size, linger.ms, and compression to improve throughput
- Simulate a consumer lag spike and observe remediation options
- Compare eager vs cooperative rebalancing behavior
- Kill a broker and observe failover, ISR changes, and recovery
- Measure end-to-end latency (produce → consume)

---

## Prerequisites

- 3-broker Docker Compose Kafka cluster
- Python 3.9+: `pip install confluent-kafka`
- `htop` or `docker stats` for resource monitoring

---

## Lab Environment

```bash
# Ensure 3-broker cluster is up
docker compose up -d
docker compose ps

# Create perf topic (many partitions for parallelism)
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic perf-test \
  --partitions 12 --replication-factor 3

docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic perf-test-result \
  --partitions 12 --replication-factor 3
```

---

## Exercise 1 — Baseline Throughput Benchmark

### 1.1 Producer benchmark — default settings

```bash
echo "=== BASELINE: default settings ==="
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    acks=1

# Record: ??? records/sec, ??? MB/sec
```

### 1.2 Consumer benchmark — baseline

```bash
docker exec kafka-1 kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic perf-test \
  --messages 1000000 \
  --group bench-consumer-1 \
  --timeout 60000
```

---

## Exercise 2 — Tuning for Throughput

### 2.1 Larger batches + linger + compression

```bash
echo "=== TUNED: batch.size=128KB + linger.ms=20 + lz4 ==="
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    acks=1 \
    batch.size=131072 \
    linger.ms=20 \
    compression.type=lz4 \
    buffer.memory=67108864
```

### 2.2 Try different compression algorithms

```bash
for compression in none snappy lz4 zstd gzip; do
  echo -n "compression=$compression: "
  docker exec kafka-1 kafka-producer-perf-test.sh \
    --topic perf-test \
    --num-records 500000 \
    --record-size 1024 \
    --throughput -1 \
    --producer-props \
      bootstrap.servers=localhost:9092 \
      acks=1 \
      batch.size=131072 \
      linger.ms=20 \
      compression.type=$compression \
    2>&1 | grep "records/sec"
done
```

### 2.3 Record your results

| Configuration | Records/sec | MB/sec | Notes |
|---------------|------------|--------|-------|
| Default (acks=1) | | | |
| batch=128KB + linger=20ms + lz4 | | | |
| batch=128KB + linger=20ms + zstd | | | |
| acks=all + batch=128KB + lz4 | | | |

**Questions:**
1. What was the throughput improvement from batching + compression?
2. What is the throughput penalty for `acks=all` vs `acks=1`?
3. Which compression algorithm gave the best throughput for random 1KB records?

---

## Exercise 3 — Consumer Lag Simulation

### 3.1 Start a slow consumer

```python
# slow_consumer.py
from confluent_kafka import Consumer
import time, json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'slow-consumer-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
})
consumer.subscribe(['perf-test'])

print("Slow consumer running (100ms processing delay per record)...")
count = 0
try:
    while True:
        msg = consumer.poll(0.1)
        if msg and not msg.error():
            time.sleep(0.1)  # simulate slow processing
            count += 1
            if count % 100 == 0:
                print(f"  Processed {count} records")
except KeyboardInterrupt:
    consumer.close()
```

```bash
python slow_consumer.py &
SLOW_PID=$!
```

### 3.2 Run a fast producer

```bash
# Produce 50,000 records quickly
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 50000 \
  --record-size 512 \
  --throughput 10000 \
  --producer-props bootstrap.servers=localhost:9092 acks=1 \
  &
```

### 3.3 Watch the lag grow

```bash
# Watch lag in real time
for i in $(seq 1 20); do
  sleep 3
  docker exec kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group slow-consumer-group 2>/dev/null \
    | awk 'NR==1 || /perf-test/' \
    | grep -v "^$"
  echo "---"
done
```

### 3.4 Fix: add more consumers to the group

```bash
# Start 3 additional fast consumers
for i in 1 2 3; do
  python3 -c "
from confluent_kafka import Consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'slow-consumer-group',
    'auto.offset.reset': 'latest',
})
consumer.subscribe(['perf-test'])
count = 0
while count < 10000:
    msg = consumer.poll(0.5)
    if msg and not msg.error():
        count += 1
consumer.close()
print(f'Consumer $i processed {count} records')
" &
done
```

```bash
# Watch lag decrease
watch -n 3 "docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group slow-consumer-group 2>/dev/null"

kill $SLOW_PID
```

**Questions:**
1. How quickly did lag grow with the slow consumer?
2. How quickly did lag decrease when additional consumers were added?
3. What was the lag per partition with 4 consumers?

---

## Exercise 4 — Eager vs Cooperative Rebalancing

### 4.1 Set up an eager rebalancing consumer group

```python
# eager_consumer.py
from confluent_kafka import Consumer
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'eager-rebalance-group',
    'partition.assignment.strategy': 'roundrobin',  # eager
    'auto.offset.reset': 'latest',
    'session.timeout.ms': '10000',
    'heartbeat.interval.ms': '3000',
})

class RebalanceTracker:
    def on_assign(self, consumer, partitions):
        import time
        print(f"[ASSIGN] {time.strftime('%H:%M:%S')} Got {len(partitions)} partitions")
    def on_revoke(self, consumer, partitions):
        import time
        print(f"[REVOKE] {time.strftime('%H:%M:%S')} Lost {len(partitions)} partitions ← STOPPED")

tracker = RebalanceTracker()
consumer.subscribe(['perf-test'],
    on_assign=tracker.on_assign,
    on_revoke=tracker.on_revoke)

count = 0
while count < 5000:
    msg = consumer.poll(0.5)
    if msg and not msg.error():
        count += 1
consumer.close()
```

```bash
# Start 3 eager consumers simultaneously and watch rebalance logs
for i in 1 2 3; do python eager_consumer.py 2>&1 | sed "s/^/[Consumer $i] /" & done
wait
```

### 4.2 Compare with cooperative rebalancing

```python
# cooperative_consumer.py — only change:
'partition.assignment.strategy': 'cooperative-sticky',  # incremental
```

Run the same test and compare:
- How many REVOKE events occurred?
- How long was processing stopped during rebalance?

**Questions:**
1. How many partitions were revoked and reassigned in the eager rebalance?
2. In cooperative mode, did any consumer stop processing entirely?
3. In what scenarios would cooperative rebalancing provide the biggest benefit?

---

## Exercise 5 — Broker Failure and Failover

### 5.1 Observe baseline state

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic perf-test \
  | grep "Leader:"
```

Note which partitions have leaders on broker 2 (`id=2`).

### 5.2 Start continuous producer and consumer

```bash
# Continuous producer (in background)
python3 -c "
from confluent_kafka import Producer
import time, json
p = Producer({'bootstrap.servers': 'localhost:9092', 'acks': '1'})
i = 0
while True:
    try:
        p.produce('perf-test', key=str(i%12), value=json.dumps({'seq': i}).encode())
        p.poll(0)
        i += 1
        if i % 1000 == 0: print(f'Produced {i}')
        time.sleep(0.001)
    except Exception as e:
        print(f'Producer error: {e}')
" &
PROD_PID=$!
```

### 5.3 Kill broker 2 and observe failover

```bash
# Kill broker 2
docker compose stop kafka-2
echo "Broker 2 stopped at $(date)"

# Watch partition leaders change
watch -n 2 "docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic perf-test 2>/dev/null \
  | grep 'Leader:' | head -12"
```

**Observe:**
- Under-replicated partitions appear immediately
- Leadership migrates to remaining brokers within seconds

### 5.4 Restore broker 2

```bash
docker compose start kafka-2
echo "Broker 2 restarted at $(date)"

# Watch ISR recover
watch -n 3 "docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic perf-test 2>/dev/null \
  | grep 'Isr:' | head -6"

kill $PROD_PID
```

**Questions:**
1. How long did leader election take after broker failure?
2. Were any messages lost during the failover?
3. How long until the ISR was fully restored after broker restart?
4. What would happen if we had `acks=all` and `min.insync.replicas=3` with only 2 brokers alive?

---

## Exercise 6 — End-to-End Latency Measurement

```python
# e2e_latency.py
from confluent_kafka import Producer, Consumer
import time, json, threading, statistics

latencies = []

def produce_with_timestamps():
    p = Producer({'bootstrap.servers': 'localhost:9092', 'acks': '1'})
    for i in range(1000):
        event = {'seq': i, 'produce_ts': time.time() * 1000}
        p.produce('perf-test', key=str(i), value=json.dumps(event).encode())
        p.poll(0)
        time.sleep(0.01)
    p.flush()

def consume_and_measure():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'latency-test',
        'auto.offset.reset': 'latest',
    })
    c.subscribe(['perf-test'])
    count = 0
    while count < 1000:
        msg = c.poll(1.0)
        if msg and not msg.error():
            data = json.loads(msg.value())
            if 'produce_ts' in data:
                latency_ms = (time.time() * 1000) - data['produce_ts']
                latencies.append(latency_ms)
                count += 1
    c.close()

# Run both in parallel
consumer_thread = threading.Thread(target=consume_and_measure)
consumer_thread.start()
time.sleep(0.5)  # let consumer subscribe first
produce_with_timestamps()
consumer_thread.join()

if latencies:
    print(f"E2E Latency (ms):")
    print(f"  p50:  {statistics.median(latencies):.1f}")
    print(f"  p95:  {sorted(latencies)[int(len(latencies)*0.95)]:.1f}")
    print(f"  p99:  {sorted(latencies)[int(len(latencies)*0.99)]:.1f}")
    print(f"  max:  {max(latencies):.1f}")
```

```bash
python e2e_latency.py
```

**Questions:**
1. What is the p99 end-to-end latency?
2. How does increasing `linger.ms` affect latency?
3. What is the trade-off between latency and throughput?

---

## Lab Summary

You have performed:

- Baseline and tuned throughput benchmarks (producer + consumer)
- Consumer lag simulation and remediation by adding consumers
- Eager vs cooperative rebalancing comparison
- Broker failure simulation with automatic failover observation
- End-to-end latency measurement

**Key takeaway:** Kafka's performance is highly tunable. The right settings depend on your SLA — high throughput favors large batches and linger, low latency favors small batches and no linger. Reliability comes from replication and cooperative rebalancing.

---

## Review Questions

1. What is the throughput trade-off between `acks=1` and `acks=all`?
2. How does cooperative rebalancing improve upon eager rebalancing?
3. After a broker failure, what determines how quickly a new leader is elected?
4. At what point does adding more consumers stop reducing lag?

---

## What's Next

**Module 7** covers Kafka security — TLS, SASL authentication, ACLs, and enterprise governance practices.

