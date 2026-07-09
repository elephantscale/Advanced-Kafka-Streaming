# Lab 9 (Optional) — Zero-Downtime Rolling Upgrade

- **Module:** Optional / Day 4 (pairs with Module 6 "Upgrading Kafka in the KRaft era")
- **Duration:** 30–45 minutes
- **Difficulty:** Intermediate (optional — for the curious / fast finishers)
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

> ✅ **Ex 1–3 verified end-to-end.** Confirmed on the local Docker Compose stack: rolling restart
> one broker at a time spikes URP then clears it within seconds while an `acks=all` producer keeps
> going (only benign `NOT_LEADER_OR_FOLLOWER` retries, no lost availability), and the two-brokers-down
> anti-pattern produces `NOT_ENOUGH_REPLICAS` as expected. The producer and URP watch run as
> **standalone client containers** (not `docker exec` into a broker) so they survive the restarts.
>
> ✅ **Ex 4–6 verified** (local Docker Compose). Graceful stop moves leaders instantly; an
> ungraceful `kill` takes ~9–12 s (session-timeout detection) and drives the producer's max latency
> to ~9 s before recovering — no loss with `acks=all`+idempotence. Two things to know: (1) a
> restarted broker does **not** reclaim leadership, so each kill exercise first restores preferred
> leaders (baked into the steps) or the kill demonstrates nothing; (2) the `acks=1` loss in Ex 6.2
> is a real *production* exposure that usually shows `loss=0` on a fast local cluster — see the note
> there, and `tools/kafka-loss/` to measure it for real.

> **The core idea:** a rolling **restart** is a rolling **upgrade** minus the binary swap. The
> observable behavior — leadership failover, under-replicated partitions recovering, clients
> never dropping — is identical. So we teach the *procedure and the discipline* by restarting
> brokers one at a time, exactly as you would during a real version upgrade.

---

## Objectives

By the end of this lab you will be able to:

- Perform a safe rolling restart of a Kafka cluster **one broker at a time**
- Use **under-replicated partitions (URP)** as the go/no-go gate between nodes
- Show that clients keep working throughout (RF=3 + `min.insync.replicas=2` + `acks=all`)
- Demonstrate the failure mode when you **don't** wait — taking two replicas down at once
- Explain why **graceful shutdown** makes leader election fast (vs an ungraceful kill)
- See **producer message piling** during a restart and the configs that control it
- Quantify the **data-loss exposure** of `acks=1` vs the zero-loss guarantee of `acks=all`
- **Diagnose and measure** data loss on a real cluster (including Kubernetes/Strimzi)

---

## Prerequisites

- Running 3-broker Kafka cluster (from Lab 1/2)
- Optional: the `monitoring` profile up, to watch URP in Grafana as well as the CLI

---

## Lab Environment

The core 3-broker cluster is all you need. Three terminals:

| Terminal | Runs |
|----------|------|
| **T1** | the continuous producer (proves availability) |
| **T2** | the URP watch (your go/no-go gate) |
| **T3** | the rolling-restart commands |

> Optional: `docker compose --profile monitoring up -d` and watch the **"Under-Replicated
> Partitions"** panel in Grafana (`:3000`) alongside the CLI — a vivid red-then-green visual.

---

## Exercise 1 — Set up a durable topic and prove availability

> **What this shows:** A rolling upgrade only stays zero-downtime if the topic can tolerate a
> broker being gone. RF=3 keeps three copies; `min.insync.replicas=2` + producer `acks=all` means
> a write succeeds as long as **two** replicas are in sync — so one broker down is fine. We start
> a continuous producer to prove the cluster keeps accepting writes through the whole roll.
>
> **If a sharp student asks:** Why does `acks=all` matter here? With `acks=1` the producer never
> notices replica health — it just needs the leader. `acks=all` + `min.insync.replicas=2` is what
> makes the cluster *refuse* an unsafe write, which is exactly the safety property a rolling
> upgrade relies on (and the thing that breaks in Exercise 3).

### 1.1 Create a durable topic (T3)

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic upgrade.demo --partitions 6 --replication-factor 3 --config min.insync.replicas=2
```

### 1.2 Start a continuous producer (T1)

Runs ~5 minutes at 2,000 msg/s with `acks=all` — long enough to cover the roll. Run it as a
**separate client container** on the Kafka network, bootstrapping **all three** brokers:

```bash
docker run --rm --network kafka apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-producer-perf-test.sh --topic upgrade.demo \
  --num-records 600000 --record-size 200 --throughput 2000 \
  --producer-props bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092 acks=all
```

> **Why not `docker exec kafka-1 ...`?** The producer must **not** run inside a broker you are
> about to restart. In Exercise 2 you restart kafka-1; anything running via `docker exec kafka-1`
> is killed the instant that container stops (SIGKILL, exit 137), so the "continuous producer"
> would die at the first step and prove nothing. A standalone client container survives every
> broker restart, and listing all three brokers in `bootstrap.servers` lets it re-route to a live
> broker whenever one goes down.
>
> **Why `acks=all`:** it forces every write to be acknowledged by all in-sync replicas (≥
> `min.insync.replicas`). If the cluster ever can't satisfy that, the producer will *tell you* —
> that's the signal that separates a safe roll from an unsafe one. During the roll you'll see
> brief `NOT_LEADER_OR_FOLLOWER` **retry** warnings as leadership moves — that's normal and the
> producer recovers; what you must *not* see is `NOT_ENOUGH_REPLICAS` (that's Exercise 3).

### 1.3 Watch under-replicated partitions (T2)

Also query from a client container with all three brokers, so the watch keeps working even while
the broker you'd otherwise query is the one being restarted:

```bash
watch -n 2 "docker run --rm --network kafka apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --describe --under-replicated-partitions"
```

At rest this is **empty** (URP = 0) — every partition has all 3 replicas in sync.

> **Same reason as the producer:** a `docker exec kafka-1` watch goes blind exactly when you
> restart kafka-1. Querying from a standalone container that lists all three brokers always reaches
> a live one, so your go/no-go gate stays trustworthy throughout the roll.

---

## Exercise 2 — Rolling restart, one broker at a time

> **What this shows:** The actual rolling-upgrade procedure. Restart one broker; its partitions
> briefly go **under-replicated** (that replica fell behind while the broker was down), then
> recover as it rejoins and catches up. You **wait for URP to return to 0** before touching the
> next broker. Meanwhile the T1 producer keeps going — maybe a brief latency blip, but no errors.
> Expect: URP spikes then clears three times; the producer never stops.
>
> **If a sharp student asks:** Why not restart all three at once to save time? Because two replicas
> of the same partition would be down together → the partition drops below `min.insync.replicas`
> and writes fail (you'll prove this in Exercise 3). The whole discipline is: one at a time,
> URP back to 0 between each.

Restart the first broker (T3), then **watch T2 until URP is empty again**:

```bash
docker compose restart kafka-1
```

Only once URP is back to 0, do the next:

```bash
docker compose restart kafka-2
```

Wait for URP = 0 again, then the last:

```bash
docker compose restart kafka-3
```

> **Why wait for URP=0 each time:** the URP-clear is proof the just-restarted broker has fully
> caught up and every partition is back to 3 in-sync replicas. Only then is it safe to remove the
> next one — this is the single rule that keeps the upgrade zero-downtime.

Check the producer terminal (T1): it kept producing throughout. **That is a zero-downtime roll.**

---

## Exercise 3 — The anti-pattern: two brokers down at once

> **What this shows:** Why the URP-zero gate exists. With RF=3 on 3 brokers, every partition has a
> replica on every broker — so stopping **two** brokers leaves each partition with a single
> replica, below `min.insync.replicas=2`. The `acks=all` producer can no longer get enough
> acknowledgements and **fails** (`NOT_ENOUGH_REPLICAS`). This is the outage an impatient upgrade
> causes.
>
> **If a sharp student asks:** Would `acks=1` have "survived" this? It wouldn't error — but it
> would be silently unsafe: a write acknowledged by the lone surviving leader is lost if that
> broker then fails. `acks=all` failing loudly is the system protecting your data, not a bug.

Restart the producer (T1) if the earlier run finished, then stop **two** brokers (T3):

```bash
docker compose stop kafka-2
docker compose stop kafka-3
```

Watch T1: the producer now reports errors (not enough in-sync replicas) — writes to affected
partitions are refused. This is exactly what a careless "restart them all" upgrade would cause.

Recover — bring both back and wait for URP to return to 0:

```bash
docker compose start kafka-2
docker compose start kafka-3
```

Once URP clears, the producer succeeds again. **Lesson: one broker at a time, always.**

---

## Exercise 4 — Graceful vs ungraceful shutdown: leader-election speed

> **What this shows:** *Why* an upgrade sometimes has a long unavailability window — and it is
> usually not Kafka being slow, it is **how the broker was stopped**. A **graceful** stop triggers
> *controlled shutdown*: the broker asks the controller to **move its partition leaderships to other
> brokers before it exits**, so new leaders are in place almost immediately. An **ungraceful** kill
> gives the controller no warning — it must first **detect** the broker is gone (missed heartbeats,
> up to `broker.session.timeout.ms` ≈ 9s) and only *then* elect new leaders. Same cluster, very
> different gap.
>
> **If a sharp student asks:** So during a rolling upgrade, always stop brokers gracefully? Yes —
> controlled shutdown (`controlled.shutdown.enable=true`, the default) is what makes leader
> hand-off fast. If your orchestration `kill`s pods/brokers, or controlled shutdown times out
> (`controlled.shutdown.max.retries`), you pay the full detection delay on every node — a classic
> "our upgrades are slow / cause errors" root cause.

### 4.1 Note the current leaders

```bash
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic upgrade.demo | grep Leader
```

### 4.2 Graceful stop — leaders move *before* the broker exits

```bash
docker compose stop kafka-1
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-2:9092,kafka-3:9092 --describe --topic upgrade.demo | grep Leader
```

> **Why leaders already moved:** controlled shutdown migrated kafka-1's leaderships to kafka-2/3
> *as part of stopping* — no partition waited for failure detection. Restart and wait for URP=0:

```bash
docker compose start kafka-1
```

### 4.3 Ungraceful kill — the controller must detect, then elect

First, give kafka-1 its leaderships back. A broker that just restarted (4.2) does **not** reclaim
leadership automatically, so without this step kafka-1 would lead **nothing** and the kill below
would have nothing to elect (you'd see no delay — and conclude, wrongly, that kill is fast):

```bash
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --election-type preferred --all-topic-partitions
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe --topic upgrade.demo | grep Leader
```

Confirm kafka-1 leads ~2 partitions again, then kill it and watch how long the leaders take to move:

```bash
docker compose kill kafka-1
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-2:9092,kafka-3:9092 --describe --topic upgrade.demo | grep Leader
```

> **Why this is slower:** kafka-1 died without warning, so for a few seconds the controller still
> believes it leads its partitions — until heartbeats time out, it is fenced, and leaders are
> re-elected. Re-run the `--describe` a few times and watch the leaders shift *after* the delay.

Restart and recover:

```bash
docker compose start kafka-1
```

**Takeaway for a real upgrade:** stop gracefully — it is the single biggest lever on
leader-election speed and the unavailability window.

---

## Exercise 5 — Producer piling during a restart

> **What this shows:** The producer-side symptom of that unavailability window. While a partition
> has no leader, the producer cannot send to it — records accumulate in its `buffer.memory` (32 MB
> default) and latency spikes; if the window is long enough the buffer fills and the producer
> **blocks** or errors. A **graceful** stop causes a small blip; an ungraceful **kill** causes a
> much larger latency spike — the "piling" the client sees. Expect the perf producer's **max
> latency** to jump during the kill and recover once the new leader is elected.
>
> **If a sharp student asks:** How do I stop the piling? Two levers: (1) shorten the window — stop
> gracefully (Ex4); (2) make the producer resilient — `enable.idempotence=true` (safe retries, no
> dupes), a generous `delivery.timeout.ms`, and enough `buffer.memory` to absorb the blip. But
> buffer sizing only *delays* the problem; faster leader election is the real fix.

First make sure the broker you're about to kill actually **leads** partitions — otherwise killing
it causes no piling (a broker that led nothing has no producer traffic to stall). After the Ex 4
kills, restore preferred leaders:

```bash
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --election-type preferred --all-topic-partitions
```

### 5.1 Run a producer and capture its latency profile (T1)

The producer runs as a **standalone client container** (all three brokers in `bootstrap.servers`)
so it survives the kill and re-routes to a live broker:

```bash
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-producer-perf-test.sh --topic upgrade.demo --num-records 300000 --record-size 200 --throughput 3000 --producer-props bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092 acks=all enable.idempotence=true
```

### 5.2 While it runs, kill a broker (T3)

```bash
docker compose kill kafka-1
```

Expect the perf line during the kill to show `max latency` jump to **~9 s** (the ungraceful
detection window from Ex 4) with throughput briefly collapsing, then recover — and **no** errors.

Watch the perf output (T1): the periodic lines show **max latency jump** while records pile waiting
for the new leader, then settle once election completes. Restart and recover:

```bash
docker compose start kafka-1
```

> **Why a spike, not a failure:** with `enable.idempotence` + retries the producer holds and retries
> the piled records, refreshing metadata until it finds the new leader — so you see latency, not
> loss. On a long enough stall (or a full buffer) it would start erroring; that is when the client's
> upstream backs up too.

### 5.3 Compare: repeat 5.1–5.2 with a graceful `stop` instead of `kill`

The latency spike is **much smaller** — because leadership moved before the broker left (Ex4).
**This is the demo that connects "slow election" to "messages piling on the producer."**

---

## Exercise 6 — Chances of losing data: `acks=1` vs `acks=all`

> **What this shows:** Exactly how much data an upgrade can lose, and why. With `acks=all` +
> `min.insync.replicas=2` + RF=3, a committed record is on **at least two** replicas, so stopping
> one broker cannot lose it — count in = count out, every time. With `acks=1`, a record is
> acknowledged by the **leader alone**; if that leader (the broker you restart) fails before its
> followers replicate, those acknowledged records are **gone**. Expect: `acks=all` loses nothing;
> `acks=1` can come up short.
>
> **If a sharp student asks:** Is `acks=1` loss guaranteed? No — it is a race (leader must fail
> after acking but before replicating), so you may need a couple of tries to see it. But the
> *exposure* is real and `acks=all` **eliminates** it. Also keep `unclean.leader.election.enable=false`
> so Kafka never promotes an out-of-sync replica (the other way upgrades lose data).

### 6.1 Baseline with `acks=all` — no loss

Use a **fresh, dedicated topic** so "how many did we send" (the fixed `--num-records`) can be
compared directly to "how many are stored" (sum of end offsets). Do **not** reuse `upgrade.demo` —
it already holds records from Exercises 1–5, so its offsets are not comparable to this run's count.

```bash
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --if-not-exists --topic upgrade.durable --partitions 6 --replication-factor 3 --config min.insync.replicas=2
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-producer-perf-test.sh --topic upgrade.durable --num-records 200000 --record-size 200 --throughput 5000 --producer-props bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092 acks=all &
sleep 3
docker compose kill kafka-2
```

Wait for the producer to finish (it will report `200000 records sent`), restart the broker, then
sum the stored records across partitions:

```bash
docker compose start kafka-2
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9092,kafka-3:9092 --topic upgrade.durable --time -1 | awk -F: '{s+=$3} END{print "stored =", s}'
```

`stored` equals the `200000` the producer sent — with `acks=all` every acknowledged record is on
≥ 2 replicas, so killing one broker loses **nothing**. (Verified: 200000 sent → 200000 stored.)

### 6.2 Contrast with `acks=1` on a min-ISR=1 topic

> ⚠️ **This is the exposure, but it usually will NOT reproduce on a local cluster.** `acks=1` loss
> requires the leader to die in the tiny window *after* it acks a record but *before* a follower
> replicates it. On a local/Docker cluster followers fetch within ~1 ms, so that window is
> effectively empty and you'll almost always see `loss=0` (tested repeatedly here). The exposure is
> **real in production**, where replication lags (cross-AZ, network, load) widen that window to
> thousands of records. Run this to see the mechanism and the *config*; measure the real thing on a
> real cluster with `tools/kafka-loss/measure_loss.py` (acked-vs-stored reconciliation).

```bash
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --if-not-exists --topic upgrade.lossy --partitions 6 --replication-factor 3 --config min.insync.replicas=1
# ensure kafka-1 actually LEADS partitions of this topic, or killing it exposes nothing:
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --election-type preferred --all-topic-partitions
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-producer-perf-test.sh --topic upgrade.lossy --num-records 200000 --record-size 200 --throughput 5000 --producer-props bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092 acks=1 &
sleep 3
docker compose kill kafka-1
```

Restart, then compare what the producer sent (`200000`) against what is actually stored:

```bash
docker compose start kafka-1
docker run --rm --network kafka apache/kafka:4.0.0 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-2:9092,kafka-3:9092 --topic upgrade.lossy --time -1 | awk -F: '{s+=$3} END{print "stored =", s, " (sent = 200000; any shortfall = acks=1 loss)"}'
```

> **Why `acks=1` can come up short:** records acknowledged by the killed leader that had not yet
> replicated are lost. Locally `stored` will usually still be `200000` (no observable loss — the
> replication window is too small); in production the shortfall is real. Either way the lesson is
> the *config*: with `acks=all` + min.ISR=2 + RF≥3 the exposure is **zero**; with `acks=1` you are
> exposed on every broker you stop. To quantify it on your own cluster, use the reconciliation tool
> in `tools/kafka-loss/` rather than end-offset counting.

**Takeaway:** durability during upgrades is a *config* decision — `acks=all`, `min.insync.replicas=2`,
RF≥3, `unclean.leader.election=false`. Get those right and a rolling upgrade loses nothing.

---

## Exercise 7 — Diagnose data loss on a real cluster

> **What this shows:** Exercises 1–6 proved the *config* that makes an upgrade lossless. This
> exercise is the **operational answer to "are we losing data, and why?"** on a real cluster —
> including a production **Kubernetes / Strimzi** one. The course ships a small toolkit for exactly
> this in `tools/kafka-loss/`. The key idea it enforces: separate **true loss** (the broker *acked*
> a record and then lost it) from a producer that merely **gave up** (never acked) or is **ignoring
> its send callbacks** — three different owners, one measurement.
>
> **If a sharp student asks:** Why not just compare "records produced" to "records consumed"? Because
> that conflates the three cases above and misses duplicates. The tool reconciles the exact set of
> **acked** sequence numbers against what is actually stored, so `LOST`, `FAILED`, and `DUPES` are
> reported separately — the difference between "fix the cluster", "fix the app", and "enable
> idempotence".

### 7.1 Scan the cluster for the settings that cause loss (read-only)

`diagnose.sh` checks, in order of how often they are the culprit: `unclean.leader.election.enable`,
`min.insync.replicas`, replication factor, under-replicated / under-min-isr partitions, Strimzi
**ephemeral storage** (wipes a broker's log on pod restart), and *how* brokers are being restarted.

```bash
# This local Docker Compose cluster:
KCLI_PREFIX="docker exec kafka-1 " BOOTSTRAP=localhost:9092 \
  tools/kafka-loss/diagnose.sh --topic upgrade.demo

# A production Strimzi cluster (auto-detects a broker pod in the namespace):
NS=<your-namespace> tools/kafka-loss/diagnose.sh --topic <your-topic>
```

### 7.2 Measure loss during a roll — produce, then verify after recovery

Produce sequence-numbered records with `acks=all` **while the cluster is being rolled**, then
reconcile **after** it is healthy (URP=0) so an in-flight unavailable partition can't be misread as
loss.

> **Activate the virtualenv first** — `source .venv/bin/activate` (created per `labs/SETUP.md`).
> `measure_loss.py` imports `confluent-kafka`, which lives in the venv, not the system Python.

```bash
# (a) during the roll — remembers exactly which records the broker acked:
python3 tools/kafka-loss/measure_loss.py --bootstrap localhost:9092,localhost:9093,localhost:9094 \
  --topic loss.probe --create --count 200000 --rate 3000 --produce-only --acked-file /tmp/acked.json

# (b) once URP is back to 0 — reconcile acked vs stored:
python3 tools/kafka-loss/measure_loss.py --bootstrap localhost:9092,localhost:9093,localhost:9094 \
  --topic loss.probe --verify-only --acked-file /tmp/acked.json
```

Read the result:

| Result | Cause | Fix |
|--------|-------|-----|
| `LOST > 0` (acked but missing) | unclean election / min.insync=1 / RF<3 / ephemeral / unsafe roll | fix the setting 7.1 flagged |
| `LOST = 0`, `FAILED > 0` | producer gave up or ignores callbacks | raise `delivery.timeout.ms`, handle send errors |
| `DUPES > 0` | idempotence off | set `enable.idempotence=true` |
| all zero | no loss from this test | look at the consumer side (commit-before-process) |

> **Production note:** the `acks=1` loss from Exercise 6.2 barely shows on a fast local cluster, but
> is real under production replication lag — `measure_loss.py` is what catches it there, because it
> tracks acked records exactly instead of counting end offsets. For Strimzi TLS/SASL setup and a
> `kubectl run` probe-pod recipe, see `tools/kafka-loss/README.md` and `ERICSSON.md`.

---

## Stretch — A real version bump

> **What this shows:** A true upgrade adds one step to the rolling restart — swapping the broker
> image — plus a final feature-flag bump. The *procedure* is identical to Exercise 2.
>
> **If a sharp student asks:** Why bump `metadata.version` last? It gates cluster-wide features;
> raise it before every node runs the new binary and the still-old nodes can't parse the new
> metadata and fail. All binaries first, feature flag last.

*(Instructor-verify first — needs a valid newer Kafka 4.x image tag.)*

1. Override one broker's `image:` in `docker-compose.yml` to a newer `apache/kafka:4.x` tag.
2. Recreate just that broker and wait for URP = 0:
   ```bash
   docker compose up -d kafka-1
   ```
3. Repeat for `kafka-2`, then `kafka-3` — one at a time, URP=0 between each.
4. Once all nodes run the new binary, raise the metadata level:
   ```bash
   docker exec kafka-1 kafka-features.sh --bootstrap-server localhost:9092 describe
   docker exec kafka-1 kafka-features.sh --bootstrap-server localhost:9092 upgrade --metadata <new-version>
   ```

---

## Cleanup

Nothing to tear down — the cluster is back to full health once all brokers are up and URP = 0.
Stop the producer (Ctrl+C in T1) and the watch (Ctrl+C in T2). Optionally delete the demo topic:

```bash
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic upgrade.demo
```

---

## Lab Summary

You performed a zero-downtime rolling restart — the same procedure as a rolling upgrade minus the
binary swap — restarting brokers one at a time and using URP=0 as the go/no-go gate. You saw the
cluster stay available throughout (RF=3 + `min.insync.replicas=2` + `acks=all`), and the failure
mode when two replicas go down together. Then you diagnosed the three problems that make real
upgrades painful: **slow leader election** (fixed by graceful/controlled shutdown), **messages
piling on the producer** (the symptom of the unavailability window, softened by producer
resilience), and **data loss** (zero with `acks=all` + min.ISR=2 + RF≥3 + no unclean election;
real with `acks=1`). Finally you learned to **diagnose and measure loss on a real cluster**
(`tools/kafka-loss/`) — separating true loss (acked-but-gone) from a producer that gave up or
ignores its callbacks. This is the operational discipline — and the exact config knobs — behind
every safe Kafka upgrade.

## Review Questions

1. Why must you wait for under-replicated partitions to return to 0 before restarting the next broker?
2. What combination of settings keeps the cluster available during the roll, and why?
3. What happens to an `acks=all` producer when two of three brokers are down, and why is that the *correct* behavior?
4. Why does a **graceful** shutdown produce faster leader election than an ungraceful **kill**?
5. During a restart, *why* do messages pile up on the producer, and what two levers reduce it?
6. What are the chances of losing data during an upgrade with `acks=all` + `min.insync.replicas=2` vs with `acks=1`, and why?
7. In a real version upgrade, what is the one extra step after all brokers run the new binary, and why is it last?
8. When measuring loss, what is the difference between a record that is **LOST** (acked but missing) and one that **FAILED** (never acked), and which one points at the cluster vs the producer app?
