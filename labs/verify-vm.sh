#!/usr/bin/env bash
#
# verify-vm.sh — check a student VM meets the Advanced Kafka lab spec.
# Read-only; run on each VM (or paste its output for review).
#
# Agreed spec (see the VM-provisioning email): 16 GB RAM / 4 vCPU / 30 GB disk,
# Ubuntu, Docker + Compose v2, inbound TCP 8080 open for Kafka UI.
#
set -uo pipefail

pass=0; warn=0; fail=0
ok()   { printf '  \033[1;32mPASS\033[0m  %s\n' "$*"; pass=$((pass+1)); }
wn()   { printf '  \033[1;33mWARN\033[0m  %s\n' "$*"; warn=$((warn+1)); }
bad()  { printf '  \033[1;31mFAIL\033[0m  %s\n' "$*"; fail=$((fail+1)); }
hdr()  { printf '\n\033[1m%s\033[0m\n' "$*"; }

hdr "Host"
if [ -r /etc/os-release ]; then . /etc/os-release
  [ "${ID:-}" = ubuntu ] && ok "OS: $PRETTY_NAME" || wn "OS: ${PRETTY_NAME:-unknown} (spec = Ubuntu)"
else wn "cannot read /etc/os-release"; fi
command -v curl >/dev/null 2>&1 && itype=$(TOKEN=$(curl -s -m2 -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 30" 2>/dev/null); curl -s -m2 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null) || itype=""
[ -n "$itype" ] && echo "  (EC2 instance type: $itype)"

hdr "RAM"
mem_gb=$(awk '/MemTotal/{printf "%.1f", $2/1024/1024}' /proc/meminfo)
mem_i=${mem_gb%.*}
if [ "$mem_i" -ge 16 ]; then ok "RAM: ${mem_gb} GB (>=16)"
elif [ "$mem_i" -ge 8 ]; then wn "RAM: ${mem_gb} GB (floor 8; recommended 16 — capstone/monitoring may swap)"
else bad "RAM: ${mem_gb} GB (<8 — will OOM under the 3-broker cluster)"; fi

hdr "CPU"
cpus=$(nproc)
if [ "$cpus" -ge 4 ]; then ok "vCPU: $cpus (>=4)"
elif [ "$cpus" -ge 2 ]; then wn "vCPU: $cpus (spec 4)"
else bad "vCPU: $cpus (<2)"; fi

hdr "Disk (free on /)"
disk_gb=$(df -BG --output=avail / 2>/dev/null | tail -1 | tr -dc '0-9')
if [ "${disk_gb:-0}" -ge 30 ]; then ok "Free disk: ${disk_gb} GB (>=30)"
elif [ "${disk_gb:-0}" -ge 15 ]; then wn "Free disk: ${disk_gb} GB (spec 30)"
else bad "Free disk: ${disk_gb:-?} GB (<15 — images + logs may fill it)"; fi

hdr "Docker"
if command -v docker >/dev/null 2>&1; then
  ok "docker: $(docker --version | awk '{print $3}' | tr -d ,)"
  if docker compose version >/dev/null 2>&1; then ok "compose v2: $(docker compose version --short 2>/dev/null)"
  else bad "'docker compose' v2 plugin missing (labs need it)"; fi
  if docker info >/dev/null 2>&1; then ok "docker daemon reachable without sudo"
  else wn "cannot talk to docker daemon (need 'newgrp docker' / re-login, or daemon down)"; fi
else bad "docker not installed (run labs/bootstrap.sh)"; fi

hdr "Port 8080 (Kafka UI)"
if command -v ss >/dev/null 2>&1; then
  if ss -ltn 2>/dev/null | grep -q ':8080 '; then wn "something is already listening on :8080 — Kafka UI won't bind"
  else ok ":8080 is free"; fi
else wn "ss not available; check :8080 manually"; fi
echo "  (note: inbound :8080 from the classroom must be opened in the VM's security group — not checkable from inside)"

hdr "Lab images (golden image should have these pre-pulled)"
for img in apache/kafka:4.0.0 ghcr.io/kafbat/kafka-ui:latest; do
  if command -v docker >/dev/null 2>&1 && docker image inspect "$img" >/dev/null 2>&1; then ok "image present: $img"
  else wn "image not pre-pulled: $img (first 'compose up' will download it)"; fi
done

hdr "Result"
printf '  %d PASS, %d WARN, %d FAIL\n' "$pass" "$warn" "$fail"
if [ "$fail" -gt 0 ]; then echo "  => NOT READY — address FAIL items."; exit 1
elif [ "$warn" -gt 0 ]; then echo "  => USABLE, with warnings above. The real test: 'docker compose up -d' then 'docker compose ps' all healthy."; exit 0
else echo "  => READY. Final check: 'docker compose up -d' and confirm all containers healthy."; exit 0; fi
