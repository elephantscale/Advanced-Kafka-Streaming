# Kafka producer data loss — runbook

A step-by-step for diagnosing and quantifying producer data loss on a **Strimzi / Kubernetes**
Kafka cluster. Read-only diagnosis first, then a controlled measurement, then the fix.

The two scripts referenced here live next to this file: `diagnose.sh` and `measure_loss.py`
(see `README.md` for full options). Replace every `<...>` placeholder with your values.

```
<namespace>        # Kafka namespace, e.g. kafka
<topic>            # the application topic you suspect is losing data
<bootstrap>        # in-cluster bootstrap, e.g. my-cluster-kafka-bootstrap.kafka.svc:9093
```

---

## The one distinction that decides everything

`measure_loss.py` reports three numbers — they point at three different owners:

| Result | Meaning | Owner |
|--------|---------|-------|
| **LOST** (acked but missing) | The cluster confirmed the write, then lost it. **True data loss.** | **Cluster** — a setting in Step 1 |
| **FAILED** (never acked) | The send errored/timed out. Only "loss" if the app ignores the callback. | **Producer app** |
| **DUPES** (record twice) | Idempotence is off (at-least-once). | Client config |

---

## Step 1 — Diagnose (read-only, ~1 minute)

Finds the misconfiguration before you touch anything:

```bash
NS=<namespace> ./diagnose.sh --topic <topic>
```

It checks, in order of how often they are the real culprit:

1. **`unclean.leader.election.enable`** — must be `false`. `true` lets an out-of-sync replica
   become leader and **discard acked records**. The #1 silent cause.
2. **`min.insync.replicas`** — must be `2` (with RF=3) on important topics. `1` means a write
   commits on a single replica; restart that broker and it's gone.
3. **Replication factor** — must be `≥ 3`.
4. **Under-replicated / under-min-isr / unavailable partitions** right now.
5. **Storage type** — `spec.kafka.storage.type: ephemeral` **wipes a broker's log on pod restart**.
   Production must be `persistent-claim` / `jbod`.
6. **How brokers are restarted** — the Strimzi operator rolls one at a time and waits for ISR
   (safe). A manual `kubectl rollout restart statefulset` or `kubectl delete pod` does **not** wait,
   so two replicas of a partition can go down together → drops below `min.insync.replicas` → loss.

Anything it prints in red is a candidate root cause. Most producer-loss reports are resolved here.

---

## Step 2 — Measure loss during a controlled roll

Quantifies real loss with an acked-vs-stored reconciliation. **Produce during the roll, verify
after the cluster is healthy** (so an in-flight unavailable partition can't be misread as loss).

Run it from a client with network access to the bootstrap — easiest is a throwaway pod:

```bash
kubectl -n <namespace> run loss-probe --rm -it --restart=Never --image=python:3.11-slim -- bash
# inside the pod:
pip -q install confluent-kafka
# copy measure_loss.py in (kubectl cp, a ConfigMap, or paste it), then:

# (a) START producing, then trigger your rolling restart/upgrade:
python3 measure_loss.py --bootstrap <bootstrap> --topic loss.probe --create \
    --count 300000 --rate 3000 --produce-only --acked-file /tmp/acked.json \
    $SECURITY

# (b) once diagnose.sh / kafka-topics --under-replicated-partitions shows 0, reconcile:
python3 measure_loss.py --bootstrap <bootstrap> --topic loss.probe \
    --verify-only --acked-file /tmp/acked.json \
    $SECURITY
```

### Security (Strimzi almost always needs TLS + SASL)

Export these before running (or pass the equivalent `--` flags). Pull the values from the Strimzi
secrets:

```bash
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export KAFKA_SASL_USERNAME=<kafka-user>
# password: kubectl get secret <kafka-user> -n <namespace> -o jsonpath='{.data.password}' | base64 -d
export KAFKA_SASL_PASSWORD=<...>
# CA cert: kubectl get secret <cluster>-cluster-ca-cert -n <namespace> -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/ca.crt
export KAFKA_SSL_CA_LOCATION=/tmp/ca.crt
SECURITY=""   # values above are read from the environment automatically
```

### Reading the result

| measure_loss.py | Likely cause | Action |
|-----------------|--------------|--------|
| `LOST > 0` | unclean=true / min.insync=1 / RF<3 / ephemeral / unsafe roll | Fix the setting Step 1 flagged |
| `LOST = 0`, `FAILED > 0` | producer gave up / ignores callbacks | Raise `delivery.timeout.ms`, handle send errors |
| `DUPES > 0` | idempotence off | Enable `enable.idempotence=true` |
| all zero | no loss from this test | Look at the consumer side (offset commit before processing) |

> **Note:** do not try to reproduce `acks=1` loss on a quiet or small cluster by counting end
> offsets — the acked-but-unreplicated window is tiny and you will usually see zero. This tool
> catches it because it tracks exactly which records were acked; the loss shows up under real
> replication lag (load, cross-AZ).

---

## Step 3 — The fix (make upgrade loss impossible)

Durability during a roll/upgrade is a **config decision**. Set all of these:

- Producer: **`acks=all`**, **`enable.idempotence=true`**, `delivery.timeout.ms` ≥ 120000, and the
  app **must check the delivery callback** (fire-and-forget turns FAILED into silent loss).
- Topic / broker: **`min.insync.replicas=2`**, **RF ≥ 3**, **`unclean.leader.election.enable=false`**.
- Storage: **persistent** (never `ephemeral`).
- Procedure: roll **one broker at a time**, waiting for **under-replicated partitions = 0** between
  each. Let the **Strimzi operator** do the roll — do not `kubectl delete pod` / `rollout restart`
  brokers yourself.

With those, a rolling upgrade loses **nothing**.

---

## Background / training

These map to the Advanced Kafka Streaming course labs (which run on a local Docker Compose cluster —
the concepts and `kafka-*.sh` commands are identical to Strimzi, run via `kubectl exec` into a
broker pod):

- **Lab 9 — Rolling upgrade:** the safe procedure, the two-brokers-down outage, graceful vs
  ungraceful shutdown, producer piling, and the `acks=1` vs `acks=all` durability decision.
- **Lab 5 — Reliability:** broker failure/failover, ISR recovery, `acks` and `min.insync.replicas`.
- **Lab 1 — Topology:** replication factor, ISR, `min.insync.replicas` fundamentals.
