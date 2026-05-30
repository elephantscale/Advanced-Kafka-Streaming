# Lab 4 — Anomaly Detection with Kafka Streams

**Module:** 4 — Stream Processing with Kafka Streams & ksqlDB
**Duration:** 60–75 minutes
**Difficulty:** Intermediate–Advanced

---

## Objectives

By the end of this lab you will be able to:

- Build a Kafka Streams application with stateful windowed aggregations
- Detect anomalies using statistical thresholds over sliding windows
- Route anomalous events to a separate output topic
- Query state stores interactively
- Monitor stream topology under load using JMX metrics
- Write and run a ksqlDB continuous query

---

## Prerequisites

- Java 17 (for the Kafka Streams application)
- Maven or Gradle (project provided)
- Python 3.9+ (`pip install confluent-kafka`)
- Docker Compose cluster from previous labs

---

## Lab Environment

```bash
# Start cluster with ksqlDB
docker compose --profile ksqldb up -d

# Verify ksqlDB
curl http://localhost:8088/info | python3 -m json.tool
```

---

## Exercise 1 — Telemetry Generator

### 1.1 Start a telemetry event producer

Save as `telemetry_producer.py`:

```python
import json, time, random, math
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Create topic first
import subprocess
subprocess.run(
    "docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 "
    "--create --topic sensor-data --partitions 6 --replication-factor 3 "
    "--config retention.ms=3600000",
    shell=True
)

DEVICES = [f'device-{i:03d}' for i in range(20)]

def generate_reading(device_id: str, t: float) -> dict:
    """Generate a sensor reading with occasional anomalies."""
    base_value = 50 + 10 * math.sin(t / 60)  # normal oscillation
    # 5% chance of anomaly spike
    if random.random() < 0.05:
        value = base_value + random.uniform(40, 100)  # anomaly!
        is_anomaly = True
    else:
        value = base_value + random.gauss(0, 2)  # normal noise
        is_anomaly = False
    return {
        'device_id': device_id,
        'value': round(value, 2),
        'unit': 'celsius',
        'timestamp': int(t * 1000),
        'is_anomaly_ground_truth': is_anomaly,  # for validation
    }

print("Producing telemetry... Ctrl+C to stop")
t = time.time()
try:
    while True:
        for device in DEVICES:
            reading = generate_reading(device, t)
            producer.produce(
                topic='sensor-data',
                key=device,
                value=json.dumps(reading).encode(),
                timestamp=reading['timestamp']
            )
        producer.poll(0)
        time.sleep(0.1)  # 10 events/sec per device = 200 events/sec total
        t = time.time()
except KeyboardInterrupt:
    producer.flush()
    print("Stopped.")
```

```bash
python telemetry_producer.py &
```

---

## Exercise 2 — Kafka Streams Anomaly Detector

### 2.1 Project setup

```bash
mkdir -p anomaly-detector/src/main/java/com/lab/kafka
cd anomaly-detector
cat > pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.lab</groupId>
  <artifactId>anomaly-detector</artifactId>
  <version>1.0</version>
  <properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams</artifactId>
      <version>3.7.0</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>2.0.9</version>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.16.1</version>
    </dependency>
  </dependencies>
</project>
EOF
```

### 2.2 Write the Anomaly Detector application

Save as `src/main/java/com/lab/kafka/AnomalyDetector.java`:

```java
package com.lab.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import java.time.Duration;
import java.util.*;

public class AnomalyDetector {
    static final ObjectMapper mapper = new ObjectMapper();

    record SensorReading(String device_id, double value, long timestamp) {}
    record DeviceStats(double sum, double sumSq, int count) {
        double mean() { return count == 0 ? 0 : sum / count; }
        double stddev() {
            if (count < 2) return 0;
            double variance = (sumSq / count) - (mean() * mean());
            return Math.sqrt(Math.max(0, variance));
        }
        DeviceStats update(double v) {
            return new DeviceStats(sum + v, sumSq + v * v, count + 1);
        }
        boolean isAnomaly(double value, double zThreshold) {
            return count >= 10 && stddev() > 0 &&
                Math.abs(value - mean()) > zThreshold * stddev();
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawData = builder.stream("sensor-data");

        // Parse, window aggregate, detect anomalies
        rawData
            .mapValues(v -> {
                try { return mapper.readValue(v, SensorReading.class); }
                catch (Exception e) { return null; }
            })
            .filter((k, v) -> v != null)
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(
                Duration.ofMinutes(5), Duration.ofSeconds(30)))
            .aggregate(
                () -> new DeviceStats(0, 0, 0),
                (key, reading, stats) -> stats.update(reading.value()),
                Materialized.<String, DeviceStats, WindowStore<Bytes, byte[]>>as("device-stats-store")
                    .withValueSerde(/* custom serde - simplified */ Serdes.String()
                        .deserializer() != null ? null : null)
            );

        // Simpler approach: use transform with a state store for anomaly detection
        StoreBuilder<KeyValueStore<String, String>> statsStore =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("device-running-stats"),
                Serdes.String(),
                Serdes.String()
            );
        builder.addStateStore(statsStore);

        KStream<String, String> anomalies = rawData
            .transform(() -> new AnomalyTransformer(), "device-running-stats");

        anomalies.to("anomalies");
        rawData.to("sensor-data-processed");

        System.out.println(builder.build().describe());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Thread.currentThread().join();
    }
}
```

**Note:** For the full working implementation, see the provided `AnomalyTransformer.java` in the lab repository.

### 2.3 Python-based anomaly detector (simpler alternative)

Save as `anomaly_detector_python.py`:

```python
from confluent_kafka import Consumer, Producer
import json, time, math
from collections import defaultdict, deque

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'anomaly-detector',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
})
consumer.subscribe(['sensor-data'])

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# State: sliding window of last 50 readings per device
windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=50))

Z_THRESHOLD = 3.0  # flag readings > 3 std devs from mean
anomaly_count = 0

print("Anomaly detector running... Ctrl+C to stop")
try:
    while True:
        msg = consumer.poll(0.1)
        if msg is None or msg.error():
            continue

        data = json.loads(msg.value())
        device_id = data['device_id']
        value = data['value']
        window = windows[device_id]
        window.append(value)

        if len(window) >= 10:
            values = list(window)
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            stddev = math.sqrt(variance)

            if stddev > 0 and abs(value - mean) > Z_THRESHOLD * stddev:
                anomaly = {
                    'device_id': device_id,
                    'anomalous_value': value,
                    'window_mean': round(mean, 2),
                    'window_stddev': round(stddev, 2),
                    'z_score': round(abs(value - mean) / stddev, 2),
                    'detected_at': int(time.time() * 1000),
                    'ground_truth': data.get('is_anomaly_ground_truth', False),
                }
                producer.produce('anomalies', key=device_id, value=json.dumps(anomaly).encode())
                producer.poll(0)
                anomaly_count += 1
                print(f"ANOMALY: {device_id} value={value:.1f} "
                      f"(mean={mean:.1f}, z={anomaly['z_score']})")

except KeyboardInterrupt:
    producer.flush()
    consumer.close()
    print(f"\nTotal anomalies detected: {anomaly_count}")
```

```bash
# Create anomalies topic
docker exec kafka-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic anomalies \
  --partitions 3 --replication-factor 3

python anomaly_detector_python.py &
```

---

## Exercise 3 — Consume and Validate Anomalies

```python
# validate_anomalies.py
from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'anomaly-validator',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe(['anomalies'])

true_positives = 0
false_positives = 0

print("Validating anomalies (Ctrl+C to stop)...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        anomaly = json.loads(msg.value())
        ground_truth = anomaly.get('ground_truth', False)
        if ground_truth:
            true_positives += 1
            label = "✓ TRUE POSITIVE"
        else:
            false_positives += 1
            label = "✗ FALSE POSITIVE"
        print(f"{label}: device={anomaly['device_id']} "
              f"z={anomaly['z_score']} value={anomaly['anomalous_value']}")
except KeyboardInterrupt:
    consumer.close()
    total = true_positives + false_positives
    if total > 0:
        precision = true_positives / total
        print(f"\nPrecision: {precision:.2%} ({true_positives}/{total})")
```

```bash
python validate_anomalies.py
```

**Questions:**
1. What is the precision of the anomaly detector?
2. How does changing `Z_THRESHOLD` affect precision vs recall?
3. What happens if a device produces a legitimate sustained high reading?

---

## Exercise 4 — ksqlDB Continuous Queries

### 4.1 Connect to ksqlDB

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

### 4.2 Create a stream from the sensor topic

```sql
CREATE STREAM sensor_readings (
    device_id VARCHAR KEY,
    value DOUBLE,
    unit VARCHAR,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='sensor-data',
    VALUE_FORMAT='JSON',
    TIMESTAMP='timestamp'
);
```

### 4.3 Windowed aggregation

```sql
-- Average per device per minute
CREATE TABLE device_minute_avg AS
SELECT
    device_id,
    WINDOWSTART AS window_start,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS reading_count
FROM sensor_readings
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY device_id
EMIT CHANGES;
```

### 4.4 Detect high values

```sql
-- Continuous alert stream: readings above 90
CREATE STREAM high_temp_alerts AS
SELECT
    device_id,
    value,
    timestamp,
    'HIGH_TEMP' AS alert_type
FROM sensor_readings
WHERE value > 90
EMIT CHANGES;
```

### 4.5 Pull query — current state

```sql
-- Point-in-time query (pull query)
SELECT device_id, avg_value, reading_count
FROM device_minute_avg
WHERE device_id = 'device-001';
```

**Questions:**
1. What is the difference between a push query (`EMIT CHANGES`) and a pull query?
2. How does ksqlDB's `WINDOW TUMBLING` compare to Kafka Streams' `TimeWindows`?
3. Can you join `sensor_readings` with a static reference table in ksqlDB?

---

## Exercise 5 — Consumer Group Monitoring

```bash
# Watch the consumer group lag for the anomaly detector
watch -n 3 "docker exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group anomaly-detector 2>/dev/null"
```

**Questions:**
1. Is the anomaly detector keeping up with the producer?
2. What is the lag?
3. How many consumers would you need to eliminate lag completely?

---

## Challenge Exercise (Optional)

Extend the anomaly detector to:

1. Maintain a **per-device baseline** that adapts over time (exponential moving average)
2. Route different severity levels to different topics:
   - `anomalies-critical` (z-score > 5)
   - `anomalies-warning` (z-score 3–5)
3. Emit a **heartbeat event** if a device has not sent data for 60 seconds

---

## Lab Summary

You have built:

- A real-time telemetry producer simulating IoT sensor data
- A Python stateful anomaly detector using sliding window statistics
- A ksqlDB stream and windowed aggregation table
- An anomaly validation framework measuring precision

**Key takeaway:** Stream processing transforms raw event streams into actionable signals. Choosing the right window type, state management approach, and threshold logic determines the quality of real-time decisions.

---

## Review Questions

1. What is the difference between event time and processing time? When does it matter?
2. Why do you need a state store for anomaly detection?
3. What happens to the anomaly detector's state if it crashes?
4. What is the purpose of the grace period in windowed operations?

---

## What's Next

**Module 5** dives into Kafka Connect — deploying connectors, handling errors, and integrating with S3, Elasticsearch, Flink, and Spark.

