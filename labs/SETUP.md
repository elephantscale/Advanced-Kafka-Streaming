# Labs Setup

## Cluster Model & Versions

- **Apache Kafka 4.x in KRaft mode** — ZooKeeper-free. There is no ZooKeeper in any lab.
- Labs run against a local **Docker Compose** 3-broker cluster (combined broker+controller nodes) for speed.
- The **main course environment is Strimzi on Kubernetes.** Every `kafka-*.sh` command is identical between the two — on Strimzi, run it via `kubectl exec` into a broker pod instead of `docker exec kafka-1`.
- Examples use `docker exec kafka-1 …`; some labs alias this to `k1`.

### Kafka 4 preview features

Several labs exercise Kafka 4 early-access features that must be enabled on the cluster (the provided lab cluster is pre-configured):

- **Share Groups (KIP-932)** — native queue semantics (Labs 1, 6)
- **KIP-848** next-gen consumer rebalance protocol (Labs 2, 5)
- **Eligible Leader Replicas / KIP-966** (Lab 2)

If a step reports a feature is unavailable, treat it as instructor-led and verify the cluster's `metadata.version` / feature flags with your instructor.

## Minimum Requirements

- Linux/macOS with Docker and Docker Compose
- Python 3.9+
- Java 17 (for Kafka CLI tools)
- 8+ GB RAM recommended

## Python Packages

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install confluent-kafka flask requests
```

## Bring Up Core Stack

Run from the repository root (or any folder inside it — `docker compose` searches
parent directories for `docker-compose.yml`). It will not find the file from outside
the repo.

```bash
docker compose up -d
docker compose ps
```

## Optional Profiles

Depending on your compose file, enable extras when needed:

```bash
# Kafka Connect labs
docker compose --profile connect up -d

# Monitoring labs
docker compose --profile monitoring up -d

# Stream processing labs (Apache Flink)
docker compose --profile flink up -d
```

## Verification

```bash
# Broker reachability
nc -zv localhost 9092

# Topic list
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

