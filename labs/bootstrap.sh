#!/usr/bin/env bash
#
# bootstrap.sh — one-time per-VM setup for the Advanced Kafka labs (Ubuntu).
#
# Installs everything the labs assume but a bare student VM lacks:
#   - Docker Engine + Docker Compose v2 plugin (official Docker apt repo)
#   - Java 17, Python 3 + venv, and small CLI helpers (nc, git, curl, jq)
#   - Adds the student to the `docker` group (so `docker exec ...` needs no sudo)
#   - Creates the .venv and installs the Python lab packages
#
# Safe to re-run: every step is idempotent.
#
# Usage:   ./labs/bootstrap.sh          # run as the student user; it calls sudo itself
#          (do NOT run as root — the docker-group step needs a real login user)
#
set -euo pipefail

# ---- pretty logging --------------------------------------------------------
log()  { printf '\n\033[1;34m==>\033[0m \033[1m%s\033[0m\n' "$*"; }
ok()   { printf '    \033[1;32m✓\033[0m %s\n' "$*"; }
warn() { printf '    \033[1;33m!\033[0m %s\n' "$*"; }
die()  { printf '\n\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# ---- preflight -------------------------------------------------------------
[ "$(id -u)" -eq 0 ] && die "Run as your normal user (not root/sudo). The script calls sudo where needed."
command -v sudo >/dev/null 2>&1 || die "sudo is required but not installed."

if [ -r /etc/os-release ]; then
  . /etc/os-release
else
  die "/etc/os-release not found — cannot confirm this is Ubuntu."
fi
[ "${ID:-}" = "ubuntu" ] || warn "This script targets Ubuntu; detected ID='${ID:-unknown}'. Continuing anyway."

TARGET_USER="${SUDO_USER:-$USER}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log "Bootstrapping VM for the Advanced Kafka labs"
ok  "User:         $TARGET_USER"
ok  "Ubuntu:       ${VERSION_ID:-?} (${VERSION_CODENAME:-?})"
ok  "Repo root:    $REPO_ROOT"

# ---- base packages ---------------------------------------------------------
# Note: no host JDK — the Kafka CLI tools run inside the broker containers via
# `docker exec`, so Java is not needed on the VM itself.
log "Installing base packages (Python, nc, git, jq, curl)"
sudo apt-get update -y
sudo apt-get install -y \
  ca-certificates curl gnupg git jq netcat-openbsd \
  python3 python3-venv python3-pip
ok "Base packages installed"

# ---- Docker Engine + Compose v2 --------------------------------------------
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  ok "Docker + Compose v2 already present — skipping install"
else
  log "Removing any old/conflicting Docker packages"
  for p in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
    sudo apt-get remove -y "$p" 2>/dev/null || true
  done

  log "Adding Docker's official apt repository"
  sudo install -m 0755 -d /etc/apt/keyrings
  if [ ! -f /etc/apt/keyrings/docker.asc ]; then
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc
  fi
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

  log "Installing Docker Engine, CLI, and Compose v2 plugin"
  sudo apt-get update -y
  sudo apt-get install -y \
    docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  ok "Docker installed"
fi

# ---- docker group (rootless docker CLI) ------------------------------------
if id -nG "$TARGET_USER" | tr ' ' '\n' | grep -qx docker; then
  ok "User '$TARGET_USER' already in the docker group"
else
  log "Adding '$TARGET_USER' to the docker group"
  sudo usermod -aG docker "$TARGET_USER"
  warn "Group change takes effect on next login. For THIS shell, run:  newgrp docker"
fi

# ---- ensure Docker is running ---------------------------------------------
log "Enabling and starting the Docker service"
sudo systemctl enable --now docker
ok "Docker service active"

# ---- Python lab environment ------------------------------------------------
log "Creating Python virtualenv and installing lab packages"
VENV="$REPO_ROOT/.venv"
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  ok "Created $VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet confluent-kafka flask requests
deactivate
ok "Python packages installed (confluent-kafka, flask, requests)"

# ---- verification ----------------------------------------------------------
log "Verification"
docker --version        | sed 's/^/    /'
docker compose version  | sed 's/^/    /'
python3 --version       | sed 's/^/    /'

log "Bootstrap complete"
cat <<EOF

  Next steps:
    1. If you were NOT prompted to log out, run:   newgrp docker
       (or log out and back in) so 'docker' works without sudo.
    2. Bring up the cluster from the repo root:
         cd "$REPO_ROOT"
         docker compose up -d
         docker compose ps
    3. Start Lab 1:
         less labs/01-Modern-EDA/lab-01-kafka-topology.md

EOF
