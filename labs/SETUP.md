# Labs Setup

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

# ksqlDB labs
docker compose --profile ksqldb up -d
```

## Verification

```bash
# Broker reachability
nc -zv localhost 9092

# Topic list
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

