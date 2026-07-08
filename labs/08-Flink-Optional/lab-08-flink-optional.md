# Lab 8 (Optional) — Stream Processing with Flink SQL over Kafka

- **Module:** Optional / Day 4 (pairs with Module 6 Flink + Module 7 filtering)
- **Duration:** 30–45 minutes
- **Difficulty:** Intermediate (optional — for the curious / fast finishers)
- **Kafka version:** 4.x (KRaft mode); **Flink 1.20**

> ✅ **OPTIONAL LAB — verified end-to-end.** This lab adds a Flink cluster and depends on the
> Flink ↔ Kafka connector version pairing (Flink 1.20 + `flink-sql-connector-kafka:3.3.0-1.20`).
> Verified on the local Docker Compose stack: Kafka-source and Kafka-sink tables, continuous
> filter, aggregation and tumbling-window plans, and the `INSERT INTO` ETL job (live emea events
> flow through to `flink.orders.emea` continuously). Offer it to students who finish the core
> labs early or want the modern stream-processing path.

---

## Objectives

By the end of this lab you will be able to:

- Start a Flink cluster alongside Kafka and open the Flink SQL client
- Declare a Kafka topic as a Flink **table** and run **continuous queries** over it
- Filter and aggregate a live stream with plain SQL (no application code)
- Sink a transformed stream back into a Kafka topic (continuous ETL)

---

## Prerequisites

- Running 3-broker Kafka cluster (from Lab 1/2)
- The `flink` Docker Compose profile (added to this repo)

---

## Lab Environment

Start the Flink cluster (JobManager + TaskManager):

```bash
docker compose --profile flink up -d
docker compose ps
```

Access:

- Flink Web UI: `http://localhost:8081` (watch your SQL jobs run here)
- Kafka UI: `http://localhost:8080`

> **What Flink is doing here:** Kafka stores and transports events; Flink is the **compute**.
> Flink SQL turns a Kafka topic into a table and runs a query that **never ends** — it emits
> results as new events arrive. This is the hands-on version of the Module 6 Flink slides and
> the Module 7 "declarative filtering" branch.

---

## Exercise 1 — Produce sample data and open the SQL client

> **What this shows:** A Flink SQL job reads a Kafka topic through the Kafka connector, so
> first we need a topic with some JSON events. We create `flink.orders` and produce a handful
> of region-tagged orders; Flink will read them from the earliest offset. Expect the SQL
> client prompt (`Flink SQL>`) once it connects to the JobManager.
>
> **If a sharp student asks:** Why JSON and not Avro? Flink supports both — `format='json'` is
> simplest for a demo; production often uses `avro-confluent` with the Schema Registry for
> schema enforcement (the Module 7 "schema-based filtering" point).

### 1.1 Create the topic and produce events

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic flink.orders --partitions 3 --replication-factor 3
```

```bash
printf '%s\n' '{"order_id":"o1","region":"emea","amount":120.5}' '{"order_id":"o2","region":"amer","amount":80.0}' '{"order_id":"o3","region":"emea","amount":200.0}' '{"order_id":"o4","region":"apac","amount":50.0}' '{"order_id":"o5","region":"emea","amount":75.5}' | docker exec -i kafka-1 kafka-console-producer.sh --bootstrap-server localhost:9092 --topic flink.orders
```

> **Why the `-i`:** the events are piped into the producer's stdin — `docker exec` needs `-i`
> to forward the pipe into the container, or the producer gets no input.

### 1.2 Open the Flink SQL client

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

You should get a `Flink SQL>` prompt. (Exit later with `QUIT;`.)

---

## Exercise 2 — Declare the Kafka topic as a table

> **What this shows:** A Flink table is just a schema + a connector pointing at a data source —
> here a Kafka topic. Once declared, you can query it like any SQL table, except the query is
> continuous. `scan.startup.mode = 'earliest-offset'` makes Flink read the topic from the start.
>
> **If a sharp student asks:** Is the table a copy of the data? No — it's a **view** over the
> Kafka topic. Flink doesn't store the rows; it reads them from Kafka on demand and keeps only
> the state a query needs (e.g. running aggregates).

Run in the SQL client:

```sql
CREATE TABLE orders (
  order_id STRING,
  region   STRING,
  amount   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink.orders',
  'properties.bootstrap.servers' = 'kafka-1:9092',
  'properties.group.id' = 'flink-sql',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

```sql
SELECT * FROM orders;
```

> **Why it doesn't "finish":** this runs in **streaming mode** — the result table updates live
> and the query stays open. Press `Q` to stop viewing (the query would run forever otherwise).

---

## Exercise 3 — Continuous filter (declarative WHERE)

> **What this shows:** The whole Module 7 "filtering that changes often" payoff in one line —
> change the `WHERE` clause and you have a new filter, **no code, no redeploy**. Expect only the
> three `emea` rows, and any new `emea` events you produce to `flink.orders` appear live.
>
> **If a sharp student asks:** How is this different from a consumer with an `if` statement? The
> consumer still deserializes and receives every record; Flink pushes the filter into a managed,
> scalable, checkpointed job you edit in SQL. At volume you'd also weigh header-skip filtering
> (Module 7) which avoids deserialization entirely.

```sql
SELECT * FROM orders WHERE region = 'emea';
```

Leave it running, then in **another terminal** produce one more emea order and watch it appear:

```bash
printf '%s\n' '{"order_id":"o6","region":"emea","amount":42.0}' | docker exec -i kafka-1 kafka-console-producer.sh --bootstrap-server localhost:9092 --topic flink.orders
```

Press `Q` to stop the query.

---

## Exercise 4 — Continuous aggregation (GROUP BY)

> **What this shows:** A streaming aggregation maintains running state per group and emits
> updated results as events arrive — a live materialized view. Expect one row per region with a
> count and summed amount that **update** as you produce more events.
>
> **If a sharp student asks:** Where does the running total live? In Flink's **state backend**
> (in-memory or RocksDB), checkpointed for fault tolerance — the same state machinery that makes
> Flink's exactly-once possible.

```sql
SELECT region, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total
FROM orders
GROUP BY region;
```

Press `Q` to stop.

---

## Exercise 5 — Sink a filtered stream back to Kafka (continuous ETL)

> **What this shows:** `INSERT INTO ... SELECT` submits a **continuous job** that reads one topic,
> transforms it, and writes to another — a live pipeline, defined in SQL. This is the Flink path
> for the capstone's "route/filter into per-region topics" step. Expect a job to appear in the
> Flink UI (`:8081`) and the emea events to land in the new topic.
>
> **If a sharp student asks:** Is this exactly-once end-to-end? Flink's Kafka sink supports
> exactly-once with `sink.delivery-guarantee = 'exactly-once'` (transactional writes) — at the
> cost of transaction overhead and `read_committed` consumers downstream. Default is at-least-once.

Declare the destination table and start the pipeline:

```sql
CREATE TABLE emea_orders (
  order_id STRING,
  region   STRING,
  amount   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink.orders.emea',
  'properties.bootstrap.servers' = 'kafka-1:9092',
  'format' = 'json'
);
```

```sql
INSERT INTO emea_orders SELECT * FROM orders WHERE region = 'emea';
```

The `INSERT` returns a **job id** and keeps running (see it in the Flink UI). Now verify the
output topic from a normal shell (single-line, paste-safe):

```bash
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flink.orders.emea --from-beginning --timeout-ms 10000
```

You should see the emea orders. Produce more emea events to `flink.orders` and they flow through
to `flink.orders.emea` continuously — a running ETL job.

---

## Stretch — Windowed aggregation (tumbling window)

> **What this shows:** Windowing groups events by time — the foundation of real-time metrics
> ("orders per region per 10 seconds"). Uses a processing-time attribute and Flink's `TUMBLE`
> table function.
>
> **If a sharp student asks:** Processing time vs event time? This uses **processing time** (the
> clock when Flink sees the record) for simplicity. Production usually uses **event time** (a
> timestamp in the record) with **watermarks** to handle out-of-order/late data correctly.

Declare a table with a processing-time column, then run a tumbling-window count:

```sql
CREATE TABLE orders_t (
  order_id STRING,
  region   STRING,
  amount   DOUBLE,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink.orders',
  'properties.bootstrap.servers' = 'kafka-1:9092',
  'properties.group.id' = 'flink-sql-win',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

```sql
SELECT region, window_start, window_end, COUNT(*) AS cnt
FROM TABLE(TUMBLE(TABLE orders_t, DESCRIPTOR(proc_time), INTERVAL '10' SECONDS))
GROUP BY region, window_start, window_end;
```

---

## Cleanup

Stop and remove **only** the Flink containers, leaving the core Kafka cluster
(and any other profiles) untouched:

```bash
docker compose rm -sf flink-jobmanager flink-taskmanager
```

> ⚠️ **Do not run `docker compose --profile flink down` for this.** `down` tears down
> the *entire* project — it removes the core kafka-1/2/3 brokers and kafka-ui too (and
> since broker storage is container-local here, that wipes your topics). The targeted
> `rm -sf` above stops and removes just the two Flink services.

---

## Lab Summary

You used Flink SQL to turn Kafka topics into tables and run continuous queries: a live filter,
a running aggregation, and a topic-to-topic ETL job — all in SQL, no application code. This is
the modern successor to ksqlDB and the "logic changes often" branch of the Module 7 filtering
decision tree.

## Review Questions

1. Why does a Flink `SELECT` over a Kafka table never "finish"?
2. What is the difference between a Flink table and a copy of the topic's data?
3. When would you choose Flink SQL filtering over header-based filtering (Module 7)?
4. What does `INSERT INTO ... SELECT` create, and where can you watch it run?
