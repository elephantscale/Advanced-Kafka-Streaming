# Lab 5 — Reliability, Scaling & Performance

- **Module:** 5 — Reliability, Scaling & Performance
- **Duration:** 75–90 minutes
- **Difficulty:** Intermediate–Advanced
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

---

## Objectives

By the end of this lab you will be able to:

- Establish producer and consumer throughput baselines
- Tune `batch.size`, `linger.ms`, and compression to improve throughput
- Simulate a consumer lag spike and apply correct remediation
- Compare eager vs cooperative rebalancing behavior
- Add brokers and perform partition reassignment without data loss
- Kill a broker and observe failover, ISR recovery, and leadership rebalance
- Measure end-to-end latency at different tuning settings

---

## Prerequisites

- 3-broker Docker Compose Kafka cluster
- Python 3.9+: `pip install confluent-kafka`
- `htop` or `docker stats` available for resource monitoring

---

## Lab Environment

> **Lab environment** — same across all seven labs
>
> - Apache **Kafka 4.x in KRaft mode** — ZooKeeper-free.
> - Local **Docker Compose** cluster. The main course runs on **Strimzi (Kubernetes)**, where every `kafka-*.sh` command is identical — just run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
> - Some labs use Kafka 4 preview features (**Share Groups / KIP-932**, **KIP-848** rebalance protocol, **ELR / KIP-966**) that must be enabled on the cluster. If a step reports one unavailable, treat it as instructor-led.
> - Full setup and prerequisites: `labs/SETUP.md`.

```bash
docker compose up -d
docker compose ps

# Create benchmark topic with adequate parallelism
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic perf-test \
  --partitions 12 --replication-factor 3
```

---

## Exercise 1 — Baseline Throughput Benchmark

### 1.1 Producer benchmark — default settings

```bash
echo "=== BASELINE: default settings (acks=1) ==="
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    acks=1
```

Record: `____ records/sec`, `____ MB/sec`

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

### 2.2 Compare compression algorithms

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
|---|---|---|---|
| Default (acks=1) | | | |
| batch=128KB + linger=20ms + lz4 | | | |
| batch=128KB + linger=20ms + zstd | | | |
| acks=all + batch=128KB + lz4 | | | |

**Questions:**
1. What was the throughput improvement from batching + compression?
2. What is the throughput penalty for `acks=all` vs `acks=1`?
3. Which compression algorithm gave the best throughput for random 1KB records?

---

## Exercise 3 — Consumer Lag and Remediation

### 3.1 Start a slow consumer

```python
# slow_consumer.py
from confluent_kafka import Consumer
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'slow-consumer-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
})
consumer.subscribe(['perf-test'])

count = 0
try:
    while True:
        msg = consumer.poll(0.1)
        if msg and not msg.error():
            time.sleep(0.1)  # simulate slow processing
            count += 1
            if count % 100 == 0:
                print(f'  Processed {count} records')
except KeyboardInterrupt:
    consumer.close()
```

```bash
python slow_consumer.py &
SLOW_PID=$!

# Produce 50,000 records at speed
docker exec kafka-1 kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 50000 \
  --record-size 512 \
  --throughput 10000 \
  --producer-props bootstrap.servers=localhost:9092 acks=1 &
```

### 3.2 Watch lag grow then remediate

```bash
# Watch lag
for i in $(seq 1 10); do
  sleep 5
  docker exec kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group slow-consumer-group 2>/dev/null \
    | awk 'NR==1 || /perf-test/'
  echo "---"
done

# Add 3 faster consumers to drain the lag
for i in 1 2 3; do
  docker exec kafka-1 kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic perf-test \
    --group slow-consumer-group \
    --timeout-ms 20000 > /dev/null &
done
wait
kill $SLOW_PID 2>/dev/null
```

**Questions:**
1. How quickly did lag grow with the slow consumer?
2. How quickly did lag decrease when additional consumers were added?
3. At what point does adding more consumers stop reducing lag further?

---

## Exercise 4 — Eager vs Cooperative Rebalancing

### 4.1 Eager rebalancing

```python
# eager_consumer.py
from confluent_kafka import Consumer
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'eager-rebalance-group',
    'partition.assignment.strategy': 'roundrobin',
    'auto.offset.reset': 'latest',
    'session.timeout.ms': '10000',
    'heartbeat.interval.ms': '3000',
})

class Tracker:
    def on_assign(self, c, p):
        print(f'[ASSIGN {time.strftime("%H:%M:%S")}] got {len(p)} partitions')
    def on_revoke(self, c, p):
        print(f'[REVOKE {time.strftime("%H:%M:%S")}] lost {len(p)} partitions ← STOPPED')

t = Tracker()
consumer.subscribe(['perf-test'], on_assign=t.on_assign, on_revoke=t.on_revoke)

count = 0
while count < 3000:
    msg = consumer.poll(0.5)
    if msg and not msg.error():
        count += 1
consumer.close()
```

```bash
for i in 1 2 3; do python eager_consumer.py 2>&1 | sed "s/^/[C$i] /" & done
wait
```

### 4.2 Cooperative rebalancing — change only this line

```python
'partition.assignment.strategy': 'cooperative-sticky',
```

Run the same test and compare:
- Number of REVOKE events
- Time processing was fully stopped during rebalance

### 4.3 New consumer protocol — KIP-848 (Kafka 4)

The first two variants are *client-side* assignment strategies. Kafka 4 adds a
*broker-coordinated* protocol (KIP-848). Switch to it by replacing the
`partition.assignment.strategy` line with the `group.protocol` setting — the broker
now owns the assignment, so no client-side assignor is configured:

```python
# Remove 'partition.assignment.strategy' and use:
'group.protocol': 'consumer',          # KIP-848 broker-driven rebalance
'group.id': 'kip848-rebalance-group',
```

Run the same 3-consumer test. Confirm which protocol the group is using:

```bash
docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group kip848-rebalance-group --state
```

**Questions:**
1. How many partitions were revoked and reassigned per rebalance in eager mode?
2. In cooperative mode, did any consumer stop processing completely?
3. Under KIP-848, what does the `--state` output report for the protocol, and how does the rebalance disruption compare to the client-side strategies?
4. In what production scenarios is the broker-driven protocol most beneficial?

---

## Exercise 5 — Expanding Kafka with No Data Loss

### 5.1 Check partition assignment before expansion

```bash
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic perf-test \
  | grep "Leader:" | awk '{print "Leader:", $4, "Replicas:", $6}'
```

### 5.2 Generate a reassignment plan to spread load

```bash
# Create topics-to-move.json
cat > /tmp/topics.json <<EOF
{"topics":[{"topic":"perf-test"}],"version":1}
EOF

docker cp /tmp/topics.json kafka-1:/tmp/topics.json

docker exec kafka-1 kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --topics-to-move-json-file /tmp/topics.json \
  --broker-list "1,2,3" \
  --generate \
  > /tmp/reassign-plan.txt

cat /tmp/reassign-plan.txt
```

### 5.3 Execute with throttle to protect live producers

```bash
# Apply reassignment with replication throttle (50 MB/s)
docker exec kafka-1 kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file /tmp/reassign-plan.txt \
  --throttle 52428800 \
  --execute

# Monitor progress
docker exec kafka-1 kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file /tmp/reassign-plan.txt \
  --verify
```

**Questions:**
1. Why throttle replication during reassignment?
2. What does `--verify` show while reassignment is in progress?
3. How do you remove the throttle after reassignment completes?

---

## Exercise 6 — Broker Failure and Failover

### 6.1 Start a continuous producer

```bash
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

### 6.2 Kill broker 2 and watch failover

```bash
docker compose stop kafka-2
echo "Broker 2 stopped at $(date)"

watch -n 2 "docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic perf-test 2>/dev/null \
  | grep 'Leader:' | head -12"
```

### 6.3 Restore and watch ISR recover

```bash
docker compose start kafka-2
echo "Broker 2 restarted at $(date)"

watch -n 3 "docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic perf-test 2>/dev/null \
  | grep 'Isr:' | head -6"

kill $PROD_PID 2>/dev/null
```

**Questions:**
1. How long did leader election take after the broker failure?
2. Were any messages lost during failover (check producer error output)?
3. How long until ISR was fully restored after broker restart?
4. What would happen with `acks=all` and `min.insync.replicas=3` when only 2 brokers are alive?

---

## Exercise 7 — End-to-End Latency Measurement

```python
# e2e_latency.py
from confluent_kafka import Producer, Consumer
import time, json, threading, statistics

latencies = []

def produce():
    p = Producer({'bootstrap.servers': 'localhost:9092', 'acks': '1'})
    for i in range(1000):
        event = {'seq': i, 'produce_ts': time.time() * 1000}
        p.produce('perf-test', key=str(i), value=json.dumps(event).encode())
        p.poll(0)
        time.sleep(0.01)
    p.flush()

def consume():
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
                latencies.append((time.time() * 1000) - data['produce_ts'])
                count += 1
    c.close()

t = threading.Thread(target=consume)
t.start()
time.sleep(0.5)
produce()
t.join()

if latencies:
    s = sorted(latencies)
    print(f'E2E Latency (ms):')
    print(f'  p50:  {statistics.median(s):.1f}')
    print(f'  p95:  {s[int(len(s)*0.95)]:.1f}')
    print(f'  p99:  {s[int(len(s)*0.99)]:.1f}')
    print(f'  max:  {max(s):.1f}')
```

```bash
python e2e_latency.py
```

**Questions:**
1. What is the p99 end-to-end latency?
2. How does increasing `linger.ms` affect p99 latency?
3. What is the fundamental trade-off between throughput and latency?

---

## Lab Summary

You performed:

- Baseline and tuned throughput benchmarks (producer + consumer)
- Compression algorithm comparison
- Consumer lag simulation and group-based remediation
- Eager vs cooperative rebalancing comparison
- Partition reassignment with throttling on a live cluster
- Broker failure simulation with automatic failover and ISR recovery
- End-to-end latency measurement at different settings

**Key takeaway:** Kafka performance is highly configurable. Throughput favors large batches and linger; low latency favors small batches. Resilience comes from replication and cooperative rebalancing. Scaling without data loss requires careful ISR validation.

---

## Review Questions

1. What is the throughput trade-off between `acks=1` and `acks=all`?
2. How does cooperative rebalancing differ from eager rebalancing, and when does it matter most?
3. After a broker failure, what determines how quickly a new leader is elected?
4. Why throttle replication during partition reassignment on a live cluster?

---

## What's Next

**Module 6** explores modern Kafka trends — edge architectures, AI-driven streaming, serverless deployments, and multi-cluster federation.

