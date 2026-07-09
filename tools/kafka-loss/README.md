# Kafka producer data-loss toolkit

Two scripts to answer "are we actually losing data, and why?" on a Kafka cluster —
built for **Strimzi on Kubernetes**, but they run anywhere you can reach a broker.

| Script | Answers |
|--------|---------|
| `measure_loss.py` | **Are we losing acked data, and how much?** Produces sequence-numbered records with `acks=all`, then reads the topic back and reports what the broker acknowledged but then lost. |
| `diagnose.sh` | **Why?** Scans the cluster for the settings that cause loss (unclean leader election, `min.insync.replicas`, RF, ephemeral storage, unsafe rolls) and prints the producer-side checklist. |

## The one distinction that matters

- **LOST** = broker *acknowledged* the write, but it's *gone* when you read the topic back.
  This is **true data loss** and it is a **cluster** problem.
- **FAILED** = the send was never acknowledged (errored/timed out). This is an **availability**
  gap, not loss — the producer was told it failed. It only becomes "loss" if the **app ignores
  delivery-callback errors** (fire-and-forget).
- **DUPES** = a record appears twice → **idempotence is off** (at-least-once).

`measure_loss.py` reports all three separately, so you immediately know whether to look at the
cluster, the producer app, or the client config.

## Run the measurement (during a rolling restart/upgrade)

From any host with network access to the bootstrap, or a throwaway pod:

```bash
pip install confluent-kafka
python3 measure_loss.py --bootstrap <bootstrap:9092> --topic loss.probe \
    --count 300000 --rate 3000 --create --rf 3 --min-isr 2
```

Run it **at rest first** (baseline should be `LOST=0`), then run the produce phase **while the
cluster is being rolled/upgraded** to catch loss caused by that operation. Send long enough to
span the whole roll (e.g. `--count 300000 --rate 3000` ≈ 100s).

### Recommended for a real roll: split produce and verify

If you produce *during* a disruption and read back immediately, an unavailable partition can make
the read incomplete and falsely look like loss. So **produce during the roll, verify after the
cluster is healthy again** (URP back to 0):

```bash
# 1) during the rolling restart/upgrade — saves the acked set to a file
python3 measure_loss.py --bootstrap <bootstrap> --topic loss.probe --create \
    --count 300000 --rate 3000 --produce-only --acked-file acked.json

# 2) once diagnose.sh / kafka-topics --under-replicated-partitions shows 0, reconcile
python3 measure_loss.py --bootstrap <bootstrap> --topic loss.probe \
    --verify-only --acked-file acked.json
```

`LOST > 0` from the verify step is unambiguous: the cluster acked those records during the roll
and no longer has them.

### Inside Kubernetes / Strimzi

Run it as a short-lived pod on the cluster network:

```bash
kubectl -n kafka run loss-probe --rm -it --restart=Never \
  --image=python:3.11-slim -- bash -lc '
    pip -q install confluent-kafka &&
    python3 - <<PY
# paste measure_loss.py here, or mount it via a ConfigMap
PY'
```

Easier: copy `measure_loss.py` into a ConfigMap or bake it into a small image. Bootstrap is your
in-cluster listener, e.g. `my-cluster-kafka-bootstrap.kafka.svc:9092`.

**Security (Strimzi prod usually has TLS + SASL):**

```bash
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export KAFKA_SASL_USERNAME=my-user
export KAFKA_SASL_PASSWORD=...          # from the Strimzi KafkaUser secret
export KAFKA_SSL_CA_LOCATION=/tmp/ca.crt # from the cluster-ca-cert secret
python3 measure_loss.py --bootstrap my-cluster-kafka-bootstrap.kafka.svc:9093 --topic loss.probe --create
```

## Run the diagnosis

```bash
# Strimzi: auto-detect a broker pod in the namespace
NS=kafka ./diagnose.sh --topic <your-topic>

# Explicit pod
NS=kafka POD=my-cluster-kafka-0 ./diagnose.sh --topic <your-topic>

# Any environment: give a prefix that runs a kafka *.sh tool when a tool name is appended
KCLI_PREFIX="kubectl exec -n kafka my-cluster-kafka-0 -- /opt/kafka/bin/" \
  BOOTSTRAP=localhost:9092 ./diagnose.sh --topic <your-topic>
```

## The usual causes of producer data loss (what `diagnose.sh` checks)

1. **`unclean.leader.election.enable=true`** — an out-of-sync replica becomes leader and
   **discards** records the old leader had acked. The #1 silent cause. Must be `false`.
2. **`min.insync.replicas=1`** (with `acks=all`) — a write commits on a single replica; restart
   that broker and it's gone. With RF=3, set `2`.
3. **RF < 3** — not enough copies to survive a broker loss.
4. **Ephemeral Strimzi storage** (`spec.kafka.storage.type: ephemeral`) — a pod restart wipes that
   broker's log. During a roll the replicas rebuild from scratch; combine with any of 1–3 and you
   lose data. Production must be `persistent-claim`/`jbod`.
5. **Unsafe roll** — the Strimzi operator rolls one broker at a time and waits for ISR (safe).
   A manual `kubectl rollout restart statefulset` or `kubectl delete pod` does **not** wait for
   under-replicated → 0 between brokers, so two replicas of a partition can be down together →
   drops below `min.insync.replicas` → loss. **Always wait for URP=0 between brokers.**
6. **Producer app config** — `acks=1/0`, low `delivery.timeout.ms`/`retries`, or **ignoring the
   delivery callback**. `measure_loss.py` separates this (`FAILED`) from real cluster loss (`LOST`).

## Reading the combined result

| measure_loss.py | diagnose.sh points at | Likely cause |
|---|---|---|
| `LOST > 0` | unclean=true / min.insync=1 / RF<3 / ephemeral / unsafe roll | **Cluster** — fix the flagged setting |
| `LOST = 0`, `FAILED > 0` | producer section | **App** gave up or ignores callbacks; raise `delivery.timeout.ms`, handle errors |
| `DUPES > 0` | — | Idempotence off; enable it (harmless but means at-least-once) |
| all zero | — | No loss from this test; investigate consumer side (offset commit before processing) |
