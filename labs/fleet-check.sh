#!/usr/bin/env bash
#
# fleet-check.sh — run verify-vm.sh on every student VM and print a pass/fail table.
#
# Streams verify-vm.sh over SSH, so the VMs do NOT need the repo cloned.
#
# Usage:
#   ./labs/fleet-check.sh <hosts-file> [extra ssh args...]
#
#   hosts-file : one "user@host" (or "host") per line; # comments and blanks ignored.
#   extra ssh args : passed through to ssh, e.g.  -i ~/keys/class.pem
#
# Examples:
#   ./labs/fleet-check.sh vms.txt -i ~/keys/class.pem
#   FLEET_MAX=20 ./labs/fleet-check.sh vms.txt        # more concurrency
#
set -uo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VERIFY="$SCRIPT_DIR/verify-vm.sh"
MAX="${FLEET_MAX:-10}"          # concurrent SSH sessions

usage() { sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
[ $# -ge 1 ] || usage
HOSTS_FILE="$1"; shift
SSH_OPTS=("$@")
[ -r "$HOSTS_FILE" ] || { echo "cannot read hosts file: $HOSTS_FILE" >&2; exit 1; }
[ -r "$VERIFY" ]     || { echo "cannot find verify-vm.sh next to this script" >&2; exit 1; }

LOGDIR="fleet-check-logs"
mkdir -p "$LOGDIR"

# read hosts
declare -a HOSTS=()
while IFS= read -r line; do
  line="${line%%#*}"; line="$(echo "$line" | xargs)"
  [ -n "$line" ] && HOSTS+=("$line")
done < "$HOSTS_FILE"
[ "${#HOSTS[@]}" -gt 0 ] || { echo "no hosts in $HOSTS_FILE" >&2; exit 1; }

run_one() {
  local host="$1" log="$2"
  ssh -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
      "${SSH_OPTS[@]}" "$host" 'bash -s' < "$VERIFY" > "$log" 2>&1
}

echo "Checking ${#HOSTS[@]} host(s), up to $MAX at a time..."
for idx in "${!HOSTS[@]}"; do
  safe=$(echo "${HOSTS[$idx]}" | tr -c 'A-Za-z0-9._@-' '_')
  run_one "${HOSTS[$idx]}" "$LOGDIR/${safe}.log" &
  while [ "$(jobs -rp | wc -l)" -ge "$MAX" ]; do wait -n 2>/dev/null || sleep 0.2; done
done
wait

# aggregate
printf '\n%-30s %-10s %s\n' "HOST" "STATUS" "DETAIL"
printf '%-30s %-10s %s\n'  "------------------------------" "----------" "----------------------------"
ready=0; usable=0; notready=0; unreach=0
for idx in "${!HOSTS[@]}"; do
  host="${HOSTS[$idx]}"
  safe=$(echo "$host" | tr -c 'A-Za-z0-9._@-' '_')
  log="$LOGDIR/${safe}.log"
  summary=$(grep -oE '[0-9]+ PASS, [0-9]+ WARN, [0-9]+ FAIL' "$log" 2>/dev/null | tail -1)
  if [ -z "$summary" ]; then
    status="UNREACH"; unreach=$((unreach+1))
    detail=$(tail -1 "$log" 2>/dev/null | cut -c1-40)
  elif echo "$summary" | grep -qE '[1-9][0-9]* FAIL'; then
    status="NOTREADY"; notready=$((notready+1)); detail="$summary"
  elif echo "$summary" | grep -q '0 WARN'; then
    status="READY"; ready=$((ready+1)); detail="$summary"
  else
    status="USABLE"; usable=$((usable+1)); detail="$summary"
  fi
  printf '%-30s %-10s %s\n' "$host" "$status" "$detail"
done

printf '\nTotals: %d READY, %d USABLE, %d NOTREADY, %d UNREACH\n' "$ready" "$usable" "$notready" "$unreach"
echo "Per-host output: $LOGDIR/  (open the .log for any NOTREADY/UNREACH host)"
[ $((notready + unreach)) -eq 0 ]
