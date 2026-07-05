# Docker Primer — Just Enough for the Labs

Elephant Scale

Notes:

---

## Why This Primer

Every lab in this course runs the Kafka cluster in **Docker**. You don't need to *know*
Docker — you need about **six commands**.

- This is a quick warm-up so the labs go smoothly.
- Already comfortable with Docker? Treat it as a 5-minute refresher.
- A one-page reference lives in `labs/DOCKER-CHEATSHEET.md`.

Notes:

---

## What Is Docker (in one minute)

A **container** is a lightweight, isolated package that bundles an app with everything it
needs to run — so it runs the same on any machine.

- Think **shipping container**: standard box, runs anywhere, no "works on my machine."
- Lighter than a virtual machine — shares the host OS kernel, starts in seconds.
- Our Kafka cluster = a few containers running side by side on your VM.

Notes:

---

## The Mental Model

Two ideas cover 90% of what you'll do:

- **`docker compose`** — starts and stops the **whole cluster** (3 brokers + Kafka UI) as a group.
- **`docker exec`** — runs a command **inside** a container. This is how you reach the `kafka-*.sh` tools that live inside a broker.

```
docker exec kafka-1 kafka-topics.sh --list ...
   └─ run this ─┘   └──── inside the kafka-1 container ────┘
```

Notes:

---

## The Six Commands

- **`docker compose up -d`** — start the cluster in the background (run once at the start)
- **`docker compose ps`** — list containers + health (you want 3 brokers **healthy**)
- **`docker exec kafka-1 <tool>.sh ...`** — run a Kafka CLI tool inside a broker (the workhorse)
- **`docker exec -it kafka-1 <tool>.sh ...`** — interactive (`-it`) for the console producer; `Ctrl+C` to exit
- **`docker compose logs kafka-1`** — a container's logs, for when something looks off
- **`docker compose down`** — stop and remove the whole cluster (at the very end, or to reset)

Notes:

---

## Start of Every Lab Session

```bash
cd ~/Advanced-Kafka-Streaming
docker compose up -d      # start the cluster
docker compose ps         # wait for 3 brokers to show (healthy) — ~15-30s
```

Then open the lab and begin.

Notes:

---

## Two Gotchas That Trip Everyone

**1. `permission denied ... /var/run/docker.sock`**
Your shell isn't in the `docker` group yet.

```bash
newgrp docker      # activates it in this shell (or log out and back in)
```

**2. A consumer "hangs" and won't return the prompt**
That's expected — it's waiting for new messages. Press **`Ctrl+C`** to stop it.
A `TimeoutException` on exit (from `--timeout-ms`) is **normal**, not an error.

Notes:

---

## Seeing It Visually — Kafka UI

Open **http://localhost:8080** in the browser inside your lab desktop (no login).

- Browse topics, partitions, replicas, consumer lag — graphically.
- A friendlier view of the same things the CLI shows you.

> Stuck on a command mid-lab? Open `labs/DOCKER-CHEATSHEET.md` — the six commands and
> these two gotchas, on one page.

Notes:

---
