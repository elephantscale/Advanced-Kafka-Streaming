# Docker Cheat Sheet — the only Docker you need for these labs

You don't need to *know* Docker for this course — you need six commands. Keep this open
beside the lab.

> **Mental model:** the Kafka cluster runs as containers. `docker compose` starts/stops the
> whole cluster; `docker exec` runs a command *inside* a container (that's how you reach the
> `kafka-*.sh` tools). That's 90% of it.

---

## The six commands

| Command | What it does |
|---|---|
| `docker compose up -d` | Start the cluster (3 brokers + Kafka UI) in the background. Run once at the start. |
| `docker compose ps` | List running containers + health. You want the 3 brokers `(healthy)`. |
| `docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list` | Run a Kafka CLI tool **inside** broker `kafka-1`. This is the workhorse — most lab steps look like this. |
| `docker exec -it kafka-1 kafka-console-producer.sh ...` | `-it` = interactive: for the producer, where you type messages. `Ctrl+C` to exit. |
| `docker compose logs kafka-1` | Show a container's logs — for when something looks wrong. |
| `docker compose down` | Stop and remove the whole cluster. Run at the very end (or to reset). |

---

## Start of every lab session

```bash
cd ~/Advanced-Kafka-Streaming
docker compose up -d          # start the cluster
docker compose ps             # wait until the 3 brokers show (healthy) — ~15-30s
```

## The two gotchas that trip everyone

**1. "permission denied ... /var/run/docker.sock"**
Your shell isn't in the `docker` group yet. Fix:
```bash
newgrp docker        # activates it in THIS shell
```
If that asks for a password / fails, log out and back in (fresh session picks it up).

**2. A consumer "hangs" and won't give the prompt back**
That's expected — a consumer keeps waiting for new messages. Press **`Ctrl+C`** to stop it.
(Some lab consumers use `--timeout-ms`, which auto-exits after N seconds and prints a
`TimeoutException` on the way out — that message is normal, not an error.)

---

## Handy extras

```bash
# open a shell inside a broker (to poke around)
docker exec -it kafka-1 bash

# stop one broker (labs simulate broker failure this way)
docker compose stop kafka-3
docker compose start kafka-3

# is a port free / who's on it?
docker compose ps            # shows published ports (9092-9094, 8080)

# full reset if things get weird
docker compose down && docker compose up -d
```

## Optional lab profiles (only when a lab says so)

```bash
docker compose --profile monitoring up -d   # Prometheus + Grafana (Labs 3, 7)
docker compose --profile connect    up -d   # Postgres + MinIO + Kafka Connect (Lab 4)
```

---

**Kafka UI** (visual view of topics/partitions/lag): open **http://localhost:8080** in the
browser inside your lab desktop. No login.
