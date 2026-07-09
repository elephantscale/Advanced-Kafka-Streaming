# Lab 9 (Optional) — Zero-Downtime Rolling Upgrade

- **Module:** Optional / Day 4 (pairs with Module 6 "Upgrading Kafka in the KRaft era")
- **Duration:** 15–25 minutes
- **Difficulty:** Intermediate (optional — for the curious / fast finishers)
- **Kafka version:** 4.x (KRaft mode — ZooKeeper-free)

> ⚠️ **OPTIONAL LAB — verify before class.** This lab performs a rolling restart of the running
> cluster. It is low-risk (no version changes in the core exercises), but the exact URP-recovery
> timing and the anti-pattern behavior should be **confirmed on the VM** before offering it.

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

Runs ~5 minutes at 2,000 msg/s with `acks=all` — long enough to cover the roll:

```bash
docker exec kafka-1 kafka-producer-perf-test.sh --topic upgrade.demo --num-records 600000 --record-size 200 --throughput 2000 --producer-props bootstrap.servers=localhost:9092 acks=all
```

> **Why `acks=all`:** it forces every write to be acknowledged by all in-sync replicas (≥
> `min.insync.replicas`). If the cluster ever can't satisfy that, the producer will *tell you* —
> that's the signal that separates a safe roll from an unsafe one.

### 1.3 Watch under-replicated partitions (T2)

```bash
watch -n 2 "docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions"
```

At rest this is **empty** (URP = 0) — every partition has all 3 replicas in sync.

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
cluster stay available throughout (RF=3 + `min.insync.replicas=2` + `acks=all`), and you saw the
failure mode when two replicas go down together. This is the operational discipline behind every
safe Kafka upgrade.

## Review Questions

1. Why must you wait for under-replicated partitions to return to 0 before restarting the next broker?
2. What combination of settings keeps the cluster available during the roll, and why?
3. What happens to an `acks=all` producer when two of three brokers are down, and why is that the *correct* behavior?
4. In a real version upgrade, what is the one extra step after all brokers run the new binary, and why is it last?
