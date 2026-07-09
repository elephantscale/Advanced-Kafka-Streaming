#!/usr/bin/env bash
# diagnose.sh — scan a Kafka (Strimzi/K8s) cluster for the settings that cause producer data loss.
#
# It checks, in order of how often they are the real culprit:
#   1. unclean.leader.election.enable   (broker default + per-topic)  — #1 silent data-loss cause
#   2. min.insync.replicas               (broker default + per-topic)  — must be >= 2 with RF=3
#   3. replication factor of the topic   — must be >= 3 for broker-failure tolerance
#   4. under-replicated / under-min-isr / unavailable partitions RIGHT NOW
#   5. Strimzi storage type (ephemeral vs persistent) — ephemeral loses data on pod restart
#   6. Strimzi Kafka CR replicas + config
# It also prints the producer-side settings the APP must get right (acks, idempotence, callbacks).
#
# How it runs the Kafka CLI is pluggable so it works on Strimzi, plain K8s, or docker:
#
#   Strimzi (auto-detect a broker pod in namespace $NS):
#       NS=kafka ./diagnose.sh --topic my-topic
#   Explicit pod:
#       NS=kafka POD=my-cluster-kafka-0 ./diagnose.sh --topic my-topic
#   Any environment — give a command prefix that runs a kafka *.sh tool when suffixed:
#       KCLI_PREFIX="kubectl exec -n kafka my-cluster-kafka-0 -- /opt/kafka/bin/" \
#       BOOTSTRAP=localhost:9092 ./diagnose.sh --topic my-topic
#       KCLI_PREFIX="docker exec kafka-1 " BOOTSTRAP=localhost:9092 ./diagnose.sh --topic upgrade.demo
#
# Needs: kubectl (for Strimzi auto-detect + storage check) and access to a broker's CLI tools.

set -uo pipefail
NS="${NS:-kafka}"
POD="${POD:-}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KCLI_PREFIX="${KCLI_PREFIX:-}"
TOPIC=""
STRIMZI_CR="${STRIMZI_CR:-}"   # name of the Kafka CR (auto-detected if empty)

while [ $# -gt 0 ]; do
  case "$1" in
    --topic) TOPIC="$2"; shift 2;;
    --bootstrap) BOOTSTRAP="$2"; shift 2;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done

red() { printf '\033[31mFAIL\033[0m %s\n' "$1"; }
grn() { printf '\033[32mOK  \033[0m %s\n' "$1"; }
ylw() { printf '\033[33mWARN\033[0m %s\n' "$1"; }
hdr() { printf '\n=== %s ===\n' "$1"; }

# Resolve how to invoke a kafka CLI tool. Prints the full command prefix.
if [ -z "$KCLI_PREFIX" ]; then
  if command -v kubectl >/dev/null 2>&1; then
    if [ -z "$POD" ]; then
      POD="$(kubectl -n "$NS" get pods -l strimzi.io/kind=Kafka -o name 2>/dev/null | grep -m1 kafka | sed 's|pod/||')"
      [ -z "$POD" ] && POD="$(kubectl -n "$NS" get pods -o name 2>/dev/null | grep -m1 -- '-kafka-' | sed 's|pod/||')"
    fi
    if [ -z "$POD" ]; then
      echo "Could not auto-detect a Kafka broker pod in namespace '$NS'."
      echo "Set POD=<broker-pod> or KCLI_PREFIX=... (see header)."; exit 2
    fi
    KCLI_PREFIX="kubectl exec -n $NS $POD -- /opt/kafka/bin/"
    echo "Using broker pod: $NS/$POD"
  else
    echo "kubectl not found and KCLI_PREFIX not set."; exit 2
  fi
fi
kcli() { local tool="$1"; shift; ${KCLI_PREFIX}${tool} "$@"; }

echo "Bootstrap (as seen from the CLI context): $BOOTSTRAP"

########################################################################
hdr "1. unclean.leader.election.enable (broker config)"
# TRUE here means an out-of-sync replica may be elected leader -> discards acked data.
# Discover a real broker id (Strimzi = 0,1,2; other clusters vary) — --entity-default is
# often empty, so we query an actual broker.
BID="$(kcli kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -oE 'id: [0-9]+' | head -1 | awk '{print $2}')"
BID="${BID:-0}"
echo "     (reading broker id $BID)"
BROKER_CFG="$(kcli kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --entity-type brokers --entity-name "$BID" --describe --all 2>/dev/null)"
ule="$(printf '%s\n' "$BROKER_CFG" | grep -o 'unclean.leader.election.enable=[a-z]*' | head -1)"
misr="$(printf '%s\n' "$BROKER_CFG" | grep -o 'min.insync.replicas=[0-9]*' | head -1)"
drf="$(printf '%s\n' "$BROKER_CFG" | grep -o 'default.replication.factor=[0-9]*' | head -1)"
case "$ule" in
  *=true)  red "$ule  — an out-of-sync replica can become leader and DROP acked records. Set false.";;
  *=false) grn "$ule";;
  *)       ylw "could not read unclean.leader.election.enable from broker defaults (check RBAC/CLI).";;
esac

hdr "2. min.insync.replicas (broker default)"
case "$misr" in
  *=1) ylw "$misr broker default — OK only if every important topic overrides it to 2 (see topic check). With acks=all and min.insync=1 a write commits on ONE replica and a single restart can lose it.";;
  *=2|*=3) grn "$misr";;
  *) ylw "min.insync.replicas broker default not found; checking per-topic below.";;
esac
[ -n "$drf" ] && echo "     ($drf)"

if [ -n "$TOPIC" ]; then
  hdr "3. topic '$TOPIC' — RF, min.insync.replicas, unclean override"
  TDESC="$(kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC" 2>/dev/null)"
  rf="$(printf '%s\n' "$TDESC" | grep -o 'ReplicationFactor: [0-9]*' | head -1 | awk '{print $2}')"
  if [ -n "$rf" ]; then
    if [ "$rf" -ge 3 ]; then grn "ReplicationFactor=$rf"; else red "ReplicationFactor=$rf — need >=3 to survive a broker loss without exposure."; fi
  else ylw "could not describe topic $TOPIC"; fi
  TCFG="$(kcli kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --entity-type topics --entity-name "$TOPIC" --describe 2>/dev/null)"
  t_misr="$(printf '%s\n' "$TCFG" | grep -o 'min.insync.replicas=[0-9]*' | head -1)"
  t_ule="$(printf '%s\n' "$TCFG" | grep -o 'unclean.leader.election.enable=[a-z]*' | head -1)"
  [ -n "$t_misr" ] && { case "$t_misr" in *=1) red "topic override $t_misr — too low";; *) grn "topic $t_misr";; esac; }
  [ -n "$t_ule" ]  && { case "$t_ule" in *=true) red "topic override $t_ule — data-loss risk";; *) grn "topic $t_ule";; esac; }
  [ -z "$t_misr$t_ule" ] && echo "     (no per-topic overrides; inherits broker defaults above)"
fi

hdr "4. partitions at risk RIGHT NOW"
urp="$(kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions 2>/dev/null | grep -c Partition)"
umin="$(kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --under-min-isr-partitions 2>/dev/null | grep -c Partition)"
unav="$(kcli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --unavailable-partitions 2>/dev/null | grep -c Partition)"
[ "${urp:-0}" -eq 0 ] && grn "under-replicated partitions: 0" || ylw "under-replicated partitions: $urp (a roll in progress, or a broker behind)"
[ "${umin:-0}" -eq 0 ] && grn "under-min-isr partitions: 0" || red "under-min-isr partitions: $umin — acks=all writes to these are FAILING now"
[ "${unav:-0}" -eq 0 ] && grn "unavailable partitions: 0" || red "unavailable partitions: $unav — offline, data at risk"

########################################################################
if command -v kubectl >/dev/null 2>&1; then
  hdr "5. Strimzi storage & roll safety (Kafka CR)"
  [ -z "$STRIMZI_CR" ] && STRIMZI_CR="$(kubectl -n "$NS" get kafka -o name 2>/dev/null | head -1 | sed 's|.*/||')"
  if [ -n "$STRIMZI_CR" ]; then
    echo "Kafka CR: $NS/$STRIMZI_CR"
    stype="$(kubectl -n "$NS" get kafka "$STRIMZI_CR" -o jsonpath='{.spec.kafka.storage.type}' 2>/dev/null)"
    case "$stype" in
      ephemeral) red "spec.kafka.storage.type=ephemeral — a pod restart WIPES that broker's log. On a roll, replicas rebuild from scratch; combined with any of 1–3 above this loses data. Use persistent-claim.";;
      persistent-claim|jbod) grn "storage.type=$stype (persistent)";;
      *) ylw "storage.type='$stype' — verify it is persistent (not ephemeral).";;
    esac
    reps="$(kubectl -n "$NS" get kafka "$STRIMZI_CR" -o jsonpath='{.spec.kafka.replicas}' 2>/dev/null)"
    [ -n "$reps" ] && { [ "${reps:-0}" -ge 3 ] && grn "kafka.replicas=$reps" || red "kafka.replicas=$reps — need >=3 for RF=3."; }
    echo "--- spec.kafka.config (loss-relevant keys) ---"
    kubectl -n "$NS" get kafka "$STRIMZI_CR" -o jsonpath='{.spec.kafka.config}' 2>/dev/null \
      | tr ',' '\n' | grep -iE 'unclean|insync|replication' || echo "   (none set explicitly — using Kafka defaults)"
  else
    ylw "no Strimzi Kafka CR found in namespace '$NS' (not Strimzi, or wrong NS)."
  fi

  hdr "6. how are brokers being restarted?"
  echo "The Strimzi operator rolls brokers ONE at a time and waits for ISR — that is safe."
  echo "A manual 'kubectl rollout restart statefulset ...' or 'kubectl delete pod' does NOT wait"
  echo "for under-replicated=0 between brokers → can drop a partition below min.insync → loss."
  echo "Check for recent manual rolls / multiple broker pods restarting together:"
  kubectl -n "$NS" get pods -o wide 2>/dev/null | grep -- '-kafka-' | awk '{print "   "$1"  restarts="$4"  age="$5"  "$3}'
fi

########################################################################
hdr "7. producer-side settings the APP must get right"
cat <<'EOF'
Data 'loss on the producer' is very often the app, not the cluster. Confirm in the app config:
   acks=all               (acks=1/0 lets the leader ack before replication → lost on leader restart)
   enable.idempotence=true (also gives retries + ordering; on by default in modern clients IF acks=all)
   retries high / delivery.timeout.ms >= 120000  (survive a leader election without giving up)
   AND the app must CHECK the delivery callback / future. Fire-and-forget (ignoring send errors)
   turns every FAILED send from measure_loss.py into silent 'loss' the cluster never caused.
Run measure_loss.py during your next rolling operation: LOST>0 = cluster problem (this script's
checks); FAILED>0 with LOST=0 = producer gave up / app ignores callbacks; DUPES>0 = idempotence off.
EOF
echo
echo "Done. Pair this with: python3 measure_loss.py --bootstrap $BOOTSTRAP --topic ${TOPIC:-<topic>} --create"
