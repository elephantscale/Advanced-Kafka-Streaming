# Module 7 — High-Volume Fan-Out Best Practices

Elephant Scale

---

## Module 7 Agenda

- The fan-out problem: 10M msg/sec, 10 overlapping consumers
- Topic design strategies: single topic vs pre-filtered sub-topics
- Partition key design and consumer group topology
- Efficient filtering: headers, schemas, Kafka Streams branching, Flink SQL
- Benchmark: duplication vs filtering
- KEDA autoscaling based on consumer lag

---

## The Fan-Out Problem

**Scenario:**

- A dataset arrives at **10 million messages per second**
- **10 consumer teams** each need a **different but overlapping subset**
- Goal: minimize storage and network duplication on Kafka
- Goal: minimize consumer CPU overhead for filtering unwanted messages

> This is a real-world telecom, IoT, and financial data challenge.

---

## Topic Design Options

| Design | Storage | Network | Consumer CPU | Ops Complexity |
|---|---|---|---|---|
| 1 broad topic + client-side filter | Low | Low | High | Low |
| 10 pre-filtered topics | 10× | 10× producer writes | Low | High (topic sprawl) |
| Streams-branched derived topics | Moderate | Moderate | Low | Moderate |
| 1 topic + header-based skip | Low | Low | Low–Moderate | Low |

**Rule of thumb:** topic proliferation trades CPU for storage. At 10M msg/sec, every deserialization matters.

---

## Partition Key Design

Partition by the field that co-locates related data for consumers:

```
Key = device_id         → all events for a device land on the same partition
Key = region + type     → consumers can target specific partitions
Key = customer_segment  → consumer reads only their segment's partitions
```

**Anti-pattern:** random or null keys → even distribution but no co-location, no filtering leverage.

---

## Header-Based Filtering

Producers tag messages with routing metadata:

```python
producer.produce(
    topic='telemetry.all',
    key=device_id.encode(),
    value=json.dumps(event).encode(),
    headers=[
        ('region', region.encode()),
        ('event_type', event_type.encode()),
    ]
)
```

Consumers skip by header — **no deserialization for unwanted records:**

```python
headers = dict(msg.headers() or [])
if headers.get('region', b'').decode() != MY_REGION:
    continue          # skip — no JSON parse, no CPU cost
data = json.loads(msg.value())   # only deserialize wanted messages
```

---

## Header Filtering: CPU Savings

At 10M msg/sec with 33% of messages wanted per consumer:

| Approach | Deserialization calls/sec | Relative CPU |
|---|---|---|
| Full deserialize all | 10,000,000 | 1× (baseline) |
| Header skip, deserialize wanted | 3,300,000 | ~0.33× |

> 3× CPU reduction per consumer — multiplied across 10 consumers = significant infrastructure savings.

---

## Schema-Based Filtering

Using Avro union types, consumers can deserialize only the outer envelope first:

```json
{
  "type": "record",
  "name": "TelemetryEvent",
  "fields": [
    {"name": "region", "type": "string"},
    {"name": "payload", "type": ["SensorData", "AlertData", "StatusData"]}
  ]
}
```

Consumer reads `region` from outer record → decides whether to parse `payload`.

Stronger guarantees than headers (schema-enforced) but higher coupling to Schema Registry.

---

## Kafka Streams Branching

Server-side pre-filtering — removes CPU cost from all consumers:

```java
KStream<String, TelemetryEvent> source = builder.stream("telemetry.all");

Map<String, KStream<String, TelemetryEvent>> branches = source.split()
    .branch((k, v) -> v.getRegion().equals("emea"), Branched.as("emea"))
    .branch((k, v) -> v.getRegion().equals("apac"), Branched.as("apac"))
    .branch((k, v) -> v.getRegion().equals("amer"), Branched.as("amer"))
    .defaultBranch(Branched.as("other"));

branches.get("emea").to("telemetry.emea");
branches.get("apac").to("telemetry.apac");
branches.get("amer").to("telemetry.amer");
```

---

## Flink SQL — Declarative Filtering

Declarative SQL alternative to Streams branching (Flink SQL is the 2026 direction;
ksqlDB has been de-emphasized by Confluent and Flink is in this course's lab env):

```sql
CREATE TABLE telemetry_all (
    region STRING,
    device_id STRING,
    `value` DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'telemetry.all',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

CREATE TABLE telemetry_emea WITH ('topic' = 'telemetry.emea') AS
    SELECT * FROM telemetry_all WHERE region = 'emea';
```

Continuous queries keep derived topics updated as new events arrive.

---

## Filtering Strategy Decision Tree

```
Is the wanted fraction < 50% of all messages?
    YES → header-based skip is worth it
    NO  → consider full deserialization or pre-filtered topics

Is the number of consumer variants > 5?
    YES → Streams branching reduces per-consumer cost
    NO  → header filtering may be sufficient

Is the filtering logic changing frequently?
    YES → Flink SQL (declarative, redeploy-free query changes)
    NO  → Kafka Streams branching (JVM, lower overhead)
```

---

## KEDA Autoscaler: Scale on Lag

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: telemetry-consumer-emea-scaler
spec:
  scaleTargetRef:
    name: telemetry-consumer-emea
  minReplicaCount: 1
  maxReplicaCount: 8      # capped by partition count / consumer groups
  pollingInterval: 15
  cooldownPeriod: 60
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      metricName: kafka_consumergroup_lag
      query: |
        max(kafka_consumergroup_lag{consumergroup="header-filter-emea"})
      threshold: "10000"
```

Scale up when lag > 10,000. Scale down after `cooldownPeriod` seconds of low lag.

---

## KEDA: Why Cap maxReplicaCount?

```
Topic: telemetry.all (24 partitions, 3 consumer teams)
Each team gets 24/3 = 8 partitions maximum

maxReplicaCount = 8
```

Adding more consumers than partitions **wastes resources** — idle replicas with no partitions assigned.

> **Kafka 4 caveat — Share Groups (KIP-932):** with a *share group* instead of a consumer
> group, this cap no longer applies — many share consumers can read the same partitions
> cooperatively, so you can scale workers past the partition count. For classic consumer
> groups (this lab's KEDA example), the partition cap still holds.

---

## Benchmark: Duplication vs Filtering

At 50,000 messages with 33% wanted (emea):

| Strategy | Time (s) | Throughput (msg/s) | Note |
|---|---|---|---|
| Full deserialize all 50K | ~baseline | — | All messages deserialized |
| Header-skip, deserialize emea only | ~0.33× time | ~3× faster | Skip at metadata level |

**Break-even point:** when wanted fraction > ~80%, full deserialization may be cheaper (no header overhead).

---

## Module 7 Summary

- Single broad topic + header filtering is the most storage-efficient fan-out design
- Header-based skip avoids deserialization cost for unwanted messages — 3× CPU savings at 33% selectivity
- Schema-based filtering provides stronger guarantees but tighter Schema Registry coupling
- Kafka Streams branching moves filtering to the server side — best for static, high-volume cases
- Flink SQL provides declarative filtering without redeployment (the modern successor to ksqlDB)
- KEDA autoscaling ties consumer replica count to Prometheus lag metric
- Cap `maxReplicaCount` to partition count per consumer group

---

## Lab Preview — Lab 7

**Design and Validate a Fan-Out Strategy**

You will:
1. Create a broad topic and produce events with routing headers
2. Implement header-based filtering across 3 consumer groups
3. Benchmark full deserialization vs header-skip
4. Review Kafka Streams branching pseudocode
5. Configure and test a KEDA ScaledObject tied to consumer lag

Environment: Docker Compose (3-broker Kafka, Prometheus, KEDA optional)
Time: 75–90 minutes

---

