#!/usr/bin/env bash
#
# golden-prep.sh — prepare THIS VM to be snapshotted as the student golden image.
#
# Run on the base VM (as the student user) AFTER labs/bootstrap.sh. It leaves the VM:
#   - Docker + Compose installed, lab images pre-pulled, course repo up to date
#   - NO running containers, port 8080 free
#   - passing verify-vm.sh
# Then Ashish takes the AMI/snapshot and clones it.
#
set -uo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO="$( cd "$DIR/.." && pwd )"
say() { printf '\n\033[1;34m==>\033[0m \033[1m%s\033[0m\n' "$*"; }

say "1/6  Clean slate — remove leftover containers/networks from testing"
( cd "$REPO" && docker compose down --remove-orphans 2>/dev/null || true )
cids=$(docker ps -aq)
if [ -n "$cids" ]; then
  echo "  removing $(echo "$cids" | wc -l) leftover container(s) in 3s (Ctrl+C to abort)"; sleep 3
  docker rm -f $cids
fi
docker network prune -f >/dev/null 2>&1 || true

say "2/6  Update the course repo"
( cd "$REPO" && git pull --ff-only ) || echo "  (git pull skipped/failed — check manually)"

say "3/6  Pre-pull lab images (so students don't download at class start)"
( cd "$REPO" && docker compose pull )
echo "  (only the core kafka + kafka-ui images exist today; connect/monitoring/flink"
echo "   profile images will pull here too once those profiles are added to the compose file)"

say "4/6  Verify the VM meets spec"
bash "$DIR/verify-vm.sh" || echo "  (verify reported issues above — review before snapshotting)"

say "5/6  Smoke-test: bring the cluster up, confirm healthy, take it down"
( cd "$REPO" && docker compose up -d )
sleep 5
( cd "$REPO" && docker compose ps )
echo "  ^ all should be healthy/running. Now taking it back down so the image ships idle."
( cd "$REPO" && docker compose down )

say "6/6  Trim dangling layers (keeps tagged images) + clear shell history"
docker system prune -f >/dev/null 2>&1 || true   # no -a: pulled lab images are kept
: > "$HOME/.bash_history" 2>/dev/null || true

say "Ready to snapshot"
cat <<EOF

  This VM now has: lab images pre-pulled, repo current, no running containers, 8080 free.

  Before Ashish clones it:
    - Take the AMI / snapshot from THIS instance.
    - On the launch template / security group for the clones, open inbound TCP 8080
      (Kafka UI) and 22 (SSH) to the classroom range — that is PER-VM, not baked into
      the image.
    - After cloning, spot-check the batch:  ./labs/fleet-check.sh vms.txt [-i key.pem]
EOF
