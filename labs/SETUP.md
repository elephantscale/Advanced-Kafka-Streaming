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
- 8+ GB RAM recommended

> The Kafka CLI tools (`kafka-*.sh`) run **inside** the broker containers via
> `docker exec`, so no host JDK is required.

## One-command setup (recommended)

The student VMs are bare. The fastest path is the bootstrap script, which installs
**everything** below (Docker + Compose v2, Java 17, Python venv, CLI helpers) and
adds you to the `docker` group — run it once per VM:

```bash
cd <repo root>
./labs/bootstrap.sh        # run as your normal user; it calls sudo itself — do NOT run as root
newgrp docker              # apply docker-group membership in this shell (or log out/in)
```

It is safe to re-run. The manual steps below explain what it does, and serve as a
fallback for non-Ubuntu VMs.

## Install Docker (one-time, per VM)

The student VMs do **not** ship with Docker. Run this once on each VM before
anything else. These steps are for **Ubuntu/Debian**; if your VMs are RHEL /
Amazon Linux / Fedora, ask your instructor for the `dnf`-based equivalent.

> Do **not** use `apt install docker.io` — the distro package is often too old
> and may not include the `docker compose` **v2** plugin the labs require.

```bash
# 1. Remove any old/conflicting packages (safe if none are present)
for p in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
  sudo apt-get remove -y $p 2>/dev/null || true
done

# 2. Add Docker's official apt repository
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 3. Install the engine, CLI, and the Compose v2 plugin
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 4. Run docker without sudo (so the labs' bare `docker exec ...` work)
sudo usermod -aG docker $USER
newgrp docker   # applies the group in THIS shell; otherwise log out/in
```

Verify:

```bash
docker --version          # Docker version 27.x or newer
docker compose version    # Docker Compose version v2.x  (note: "compose", no hyphen)
docker run --rm hello-world
```

> **Debian note:** in step 2 replace both `…/linux/ubuntu…` URLs with
> `…/linux/debian…`.

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

