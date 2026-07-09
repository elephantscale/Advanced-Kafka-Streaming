#!/usr/bin/env python3
"""
measure_loss.py — quantify Kafka producer data loss with a produce/consume reconciliation.

It produces N sequence-numbered records with acks=all (+ idempotence), remembering which
sequences the broker ACKNOWLEDGED, then reads the whole topic back and compares. The output
separates the two very different failure modes:

  * LOST   = acked by the broker but NOT present when we read the topic back.
             This is TRUE data loss — the cluster confirmed a write and then lost it.
             (Root causes: unclean leader election, min.insync too low, RF too low,
              ephemeral broker storage, or restarting brokers faster than ISR can recover.)
  * FAILED = never acked (send errored / timed out). This is an AVAILABILITY gap, not loss —
             the producer was TOLD the write did not happen. An app that checks delivery
             callbacks can retry these. An app that ignores callbacks turns them into silent loss.
  * DUPES  = a sequence present more than once → idempotence is OFF (at-least-once).

Run the produce phase WHILE a rolling restart / upgrade is happening on the cluster to catch
loss caused by that operation. Run it at rest first to get a zero-loss baseline.

Usage (from any host with network access to the bootstrap, or `kubectl run` a python pod):

    pip install confluent-kafka
    python3 measure_loss.py --bootstrap b-svc:9092 --topic loss.probe \
        --count 300000 --rate 3000 --create --rf 3 --min-isr 2

Security (Strimzi prod usually needs this) — set via flags or env:
    --security-protocol SASL_SSL  (env KAFKA_SECURITY_PROTOCOL)
    --sasl-mechanism SCRAM-SHA-512 (env KAFKA_SASL_MECHANISM)
    --sasl-username / --sasl-password (env KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD)
    --ssl-ca /path/ca.crt          (env KAFKA_SSL_CA_LOCATION)

Exit code is non-zero if any acked record was lost.
"""
import argparse, json, os, sys, time

try:
    from confluent_kafka import Producer, Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    sys.exit("confluent-kafka not installed. Run: pip install confluent-kafka")


def base_conf(a):
    c = {"bootstrap.servers": a.bootstrap}
    sp = a.security_protocol or os.getenv("KAFKA_SECURITY_PROTOCOL")
    if sp:
        c["security.protocol"] = sp
    mech = a.sasl_mechanism or os.getenv("KAFKA_SASL_MECHANISM")
    if mech:
        c["sasl.mechanism"] = mech
        c["sasl.username"] = a.sasl_username or os.getenv("KAFKA_SASL_USERNAME", "")
        c["sasl.password"] = a.sasl_password or os.getenv("KAFKA_SASL_PASSWORD", "")
    ca = a.ssl_ca or os.getenv("KAFKA_SSL_CA_LOCATION")
    if ca:
        c["ssl.ca.location"] = ca
    return c


def ensure_topic(a):
    admin = AdminClient(base_conf(a))
    md = admin.list_topics(timeout=15)
    if a.topic in md.topics:
        print(f"[topic] {a.topic} exists")
        return
    if not a.create:
        sys.exit(f"[topic] {a.topic} does not exist and --create not set")
    nt = NewTopic(a.topic, num_partitions=a.partitions, replication_factor=a.rf,
                  config={"min.insync.replicas": str(a.min_isr)})
    fs = admin.create_topics([nt])
    fs[a.topic].result(timeout=30)
    print(f"[topic] created {a.topic} p={a.partitions} rf={a.rf} min.insync.replicas={a.min_isr}")


def produce(a):
    acked, failed = set(), set()

    def cb(err, msg):
        seq = int(msg.key())
        (failed if err else acked).add(seq)

    conf = base_conf(a)
    conf.update({"acks": "all", "enable.idempotence": True,
                 "delivery.timeout.ms": a.delivery_timeout_ms, "linger.ms": 5})
    p = Producer(conf)
    print(f"[produce] sending {a.count} records @ ~{a.rate}/s (acks=all, idempotence=on) ...")
    interval = 1.0 / a.rate if a.rate > 0 else 0
    t0 = time.time()
    for i in range(a.count):
        while True:
            try:
                p.produce(a.topic, key=str(i), value=json.dumps({"seq": i}).encode(), callback=cb)
                break
            except BufferError:
                p.poll(0.2)
        if i % 500 == 0:
            p.poll(0)
        if interval:
            # cheap pacing that tolerates stalls during a restart
            target = t0 + (i + 1) * interval
            drift = target - time.time()
            if drift > 0:
                time.sleep(drift)
    left = p.flush(a.delivery_timeout_ms / 1000 + 30)
    if left:
        print(f"[produce] WARNING {left} messages still in queue after flush timeout")
    print(f"[produce] attempted={a.count} acked={len(acked)} failed={len(failed)} "
          f"elapsed={time.time()-t0:.1f}s")
    return acked, failed


def consume_present(a):
    conf = base_conf(a)
    conf.update({"group.id": f"loss-probe-verify-{int(time.time())}",
                 "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    c = Consumer(conf)
    md = c.list_topics(a.topic, timeout=15)
    parts = list(md.topics[a.topic].partitions.keys())
    tps = [TopicPartition(a.topic, pid) for pid in parts]
    ends = {}
    for tp in tps:
        lo, hi = c.get_watermark_offsets(tp, timeout=15)
        ends[tp.partition] = hi
    c.assign([TopicPartition(a.topic, pid, 0) for pid in parts])
    present, dupes = set(), 0
    remaining = sum(1 for pid in parts if ends[pid] > 0)
    done = set(pid for pid in parts if ends[pid] <= 0)
    print(f"[consume] reading {a.topic} to end offsets {ends} ...")
    idle = 0
    while len(done) < len(parts):
        msg = c.poll(1.0)
        if msg is None:
            idle += 1
            if idle > 20:
                print("[consume] WARNING gave up waiting (idle) — some partitions unread")
                break
            continue
        if msg.error():
            continue
        idle = 0
        try:
            seq = json.loads(msg.value())["seq"]
        except Exception:
            continue
        if seq in present:
            dupes += 1
        present.add(seq)
        if msg.offset() >= ends[msg.partition()] - 1:
            done.add(msg.partition())
    c.close()
    return present, dupes


def main():
    ap = argparse.ArgumentParser(description="Measure Kafka producer data loss (acked-but-missing).")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default="loss.probe")
    ap.add_argument("--count", type=int, default=200000)
    ap.add_argument("--rate", type=int, default=2000, help="target records/sec (0 = as fast as possible)")
    ap.add_argument("--create", action="store_true", help="create the topic if missing")
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--rf", type=int, default=3)
    ap.add_argument("--min-isr", type=int, default=2)
    ap.add_argument("--delivery-timeout-ms", type=int, default=120000)
    ap.add_argument("--security-protocol")
    ap.add_argument("--sasl-mechanism")
    ap.add_argument("--sasl-username")
    ap.add_argument("--sasl-password")
    ap.add_argument("--ssl-ca")
    ap.add_argument("--acked-file", help="persist/load the acked set (JSON) between phases")
    ap.add_argument("--produce-only", action="store_true",
                    help="produce and save acked set to --acked-file; verify later")
    ap.add_argument("--verify-only", action="store_true",
                    help="skip producing; load --acked-file and reconcile against the topic")
    a = ap.parse_args()

    if a.verify_only:
        if not a.acked_file:
            sys.exit("--verify-only requires --acked-file")
        acked = set(json.load(open(a.acked_file))["acked"])
        failed = set()
        print(f"[verify] loaded {len(acked)} acked sequences from {a.acked_file}")
    else:
        ensure_topic(a)
        acked, failed = produce(a)
        if a.acked_file:
            json.dump({"acked": sorted(acked)}, open(a.acked_file, "w"))
            print(f"[produce] saved acked set to {a.acked_file}")
        if a.produce_only:
            print("[produce-only] done. After the cluster is healthy (URP=0), run again with "
                  f"--verify-only --acked-file {a.acked_file or '<file>'}")
            return
    time.sleep(3)  # let the tail replicate before we read back
    present, dupes = consume_present(a)

    lost = sorted(acked - present)
    print("\n================ RESULT ================")
    print(f"acked (broker confirmed) : {len(acked)}")
    print(f"present in topic         : {len(present)}")
    print(f"failed (never acked)     : {len(failed)}  <- availability gap (retries needed), NOT loss")
    print(f"duplicates               : {dupes}  <- >0 means idempotence is OFF")
    print(f"LOST (acked but missing) : {len(lost)}  <- TRUE DATA LOSS")
    if lost:
        print(f"  sample lost sequences  : {lost[:20]}{' ...' if len(lost) > 20 else ''}")
        print("\n>>> The cluster acknowledged these writes and then lost them.")
        print(">>> Run diagnose.sh — the usual cause is unclean leader election,")
        print(">>> min.insync.replicas too low, ephemeral storage, or an unsafe roll.")
    else:
        print("\n>>> No acked record was lost. If the app still 'loses' data, either it is")
        print(">>> ignoring producer delivery-callback errors (the FAILED count above), or the")
        print(">>> loss is on the consumer side (offset commit before processing).")
    print("========================================")
    sys.exit(1 if lost else 0)


if __name__ == "__main__":
    main()
