# Kafka broker rightsizing, storage, network & tiered storage

Practical guidance for **on-prem / physical-server** Kafka 4.x (KRaft) deployments, with cloud
equivalents noted. Written for the Ericsson infrastructure questions; the numbers are rules of
thumb — validate against your own workload with `kafka-producer-perf-test.sh` /
`kafka-consumer-perf-test.sh` (Lab 5).

---

## 1. Broker rightsizing — best practices

Kafka's performance model is unusual: it is built around **sequential disk I/O + the OS page
cache**, so the biggest sizing lever is **RAM for page cache**, not JVM heap or raw CPU.

| Resource | Guidance | Why |
|----------|----------|-----|
| **RAM** | 64–256 GB. Keep **JVM heap small: 6–8 GB** (rarely >16 GB). Leave the rest to the OS page cache. | Reads/writes hit page cache; consumers reading recent data never touch disk. A record produced and consumed within the page-cache window is a memory-to-network copy. Large heaps just cause GC pain. |
| **CPU** | 12–32 cores. More if you use **TLS**, **compression on the broker**, or have very high connection/partition counts. | Kafka is rarely CPU-bound on the data path; TLS handshakes/encryption and (de)compression are the main CPU consumers. |
| **Disk** | NVMe/SSD, sized for `throughput × retention × RF`. JBOD (many disks) for capacity/throughput. | See §3. Faster disk = faster recovery and lower tail latency. |
| **Network** | Usually the **real** limit — see §2. 25 GbE minimum for serious clusters. | Replication (RF) + consumer fan-out multiply ingress. |
| **Partitions/broker** | Low thousands is comfortable; tens of thousands is possible on strong hardware with KRaft. | KRaft (Kafka 4.0, no ZooKeeper) removed the old cluster-wide partition ceiling. Per-broker limits are now memory (replica-fetcher state, open files) and **recovery time** after a restart. |

**Rules of thumb**
- Size to **sustained** peak, not average, and run each broker at **≤ 60–70 %** of capacity.
  When one broker fails, its leadership and replication load redistributes to the survivors — that
  headroom is what keeps you up (this is the Lab 9 / Lab 5 failover lesson in hardware terms).
- Account for **write amplification**: with RF=3, one produced byte becomes ~3 bytes written across
  the cluster and 2× replication traffic. Plan disk and network for `ingress × RF`.
- Prefer **more, smaller brokers** over a few huge ones: smaller blast radius on failure, finer
  rebalance granularity, faster individual recovery.
- Separate **controllers** (KRaft) onto dedicated nodes for large clusters (3 or 5 voters); combine
  them with brokers only for small/dev clusters.

---

## 2. Network as the limiting factor (and how to beat it)

For most well-provisioned clusters, **network — not disk — is the bottleneck**, because every byte
is multiplied:

```
cluster network ≈ ingress × ( 1                      # produce to leader
                             + (RF − 1)              # leader → followers (replication)
                             + N_consumer_groups )   # fan-out, each group reads a full copy
```

Example: 500 MB/s ingress, RF=3, 4 consumer groups → ~500 × (1 + 2 + 4) = **3.5 GB/s** of network
across the cluster. A single 10 GbE NIC (~1.25 GB/s) is saturated fast.

**Levers, roughly in order of impact:**

1. **Bigger/for more NICs.** 25/40/100 GbE. Bond multiple NICs, and — critically — **don't put two
   brokers behind one NIC** (see §5). This is often the single biggest fix.
2. **Producer compression** (`compression.type=lz4` or `zstd`). Fewer bytes on the wire *and* on
   disk, at some CPU cost. Frequently a 2–5× reduction — measure with the Lab 5 compression matrix.
3. **Rack / AZ awareness + fetch-from-follower (KIP-392).** Set `broker.rack`; consumers can read
   from the **closest** replica instead of always the leader, cutting cross-rack/cross-AZ egress.
4. **Reduce fan-out.** Every extra consumer group re-reads the whole stream. Consolidate consumers,
   or use **tiered storage** (§4) so history reads don't compete with the hot path. Kafka 4
   **share groups** (queue semantics) also decouple worker count from re-reading.
5. **OS/TCP tuning.** Raise `socket.send.buffer.bytes` / `socket.receive.buffer.bytes` and
   `num.network.threads`; enable jumbo frames (MTU 9000) on the cluster network; tune the kernel
   socket buffers. These unlock the NIC you already have.
6. **Quotas.** Producer/consumer/replication byte-rate quotas stop one client from starving the NIC.

> Keep **storage traffic off the data NIC**: if you use network-attached storage (§3), it competes
> with replication and client traffic for the same bandwidth — a common hidden bottleneck.

---

## 3. Storage options for brokers

| Option | Verdict | Notes |
|--------|---------|-------|
| **Local NVMe / SSD** | ✅ **Recommended default** | Best throughput + lowest latency + fastest recovery. Kafka's I/O is sequential, so even SATA SSD/enterprise HDD can sustain high throughput, but NVMe wins on latency and rebuild speed. Use **JBOD** (multiple independent disks) for capacity/throughput — Kafka spreads partitions across them. No RAID needed for redundancy (replication is your redundancy); RAID-0 or JBOD for performance. |
| **In-memory (tmpfs / RAM disk)** | ❌ **Avoid for the log** | Redundant and dangerous. Kafka *already* serves hot data from the **page cache** — putting the log on tmpfs just duplicates that in RAM and **destroys durability** (a broker restart wipes its log; combined with any replication weakness → data loss). Kafka's durability = replication + fsync to real disk. Give that RAM to the page cache instead. |
| **External storage appliance (SAN / NAS / iSCSI / NFS)** | ⚠️ **Discouraged for the hot path** | Adds latency, a shared failure domain, and — worst — puts **storage on the network**, competing with replication/client bandwidth (§2). Kafka's replication already provides HA, so SAN-level HA is paying twice. If you must (existing investment), give it a **dedicated** storage network and treat throughput/IOPS as a hard limit. |
| **Cloud block storage (EBS gp3/io2, PD-SSD)** | ✅ Acceptable (cloud) | The pragmatic "external" storage in cloud; AWS MSK uses EBS. Provision IOPS/throughput explicitly (gp3 lets you buy throughput). Slower recovery than local NVMe but decouples storage from instance lifecycle. |
| **Object storage (S3/GCS/Azure Blob)** | ✅ via **tiered storage only** (§4) | Not a broker log filesystem — used as the cold tier behind local disk. |

**Bottom line for physical servers:** local **NVMe in JBOD**, sized for `throughput × local-retention
× RF`, with replication (not RAID/SAN) providing redundancy.

---

## 4. Tiered storage — when to use it & retention

**Tiered storage (KIP-405)** is production-ready in Apache Kafka 4.x. Brokers keep a **hot** window
of recent segments on **local disk** and offload **older, closed** segments to **object storage**
(S3/GCS/Azure Blob/HDFS). Consumers read both transparently through the normal Kafka API.

**Use it when:**
- You need **long retention** (days → months) without buying huge local disks — cheap object storage
  holds the tail.
- You want to **decouple storage from compute** — scale retention independently of broker count.
- You want **faster recovery / rebalancing** — less data lives on local disk, so a replaced broker
  re-replicates far less (a big operational win on large brokers).
- You do occasional **replay/backfill** of large history but serve mostly recent data.

**Don't bother when:**
- Retention is short (hours) and fits comfortably on local disk.
- You need consistently **low-latency reads of old data** (object-store reads are slower/higher
  latency than local NVMe).
- Small clusters where the extra moving part isn't worth it.

**Retention config (with tiered enabled):**

| Setting | Meaning |
|---------|---------|
| `remote.storage.enable=true` | turn on tiering for the topic |
| `local.retention.ms` / `local.retention.bytes` | how long/large data stays **on local disk** — size this to your **hot window** (what active consumers actually read, e.g. a few hours) |
| `retention.ms` / `retention.bytes` | **total** retention across local **+** remote (the real data-lifetime policy) |

Pattern: `local.retention.ms` = hours (covers real-time consumers) while `retention.ms` = weeks/months
(the compliance/replay horizon in cheap object storage). Compacted topics and tiering can combine,
but tier the "hot delete" topics that dominate volume first.

---

## 5. How many brokers per physical server?

**Default and strong recommendation: one broker per physical server (one broker per failure
domain).** This is the single most important placement rule.

**Why one:**
- **Fault isolation.** A broker is a replication unit. If two brokers share a server and it dies,
  **two replicas of the same partition can go down together** → the partition drops below
  `min.insync.replicas` and you lose availability or data — exactly the Lab 9 Ex 3 / Ex 6 failure.
  Rack/replica-placement awareness assumes **broker = independent failure domain**.
- **Resource contention.** Co-located brokers fight over the same **NIC** (§2 — often the bottleneck),
  page cache, and disk queues. You rarely get 2× the throughput; you get two brokers at ~half each
  plus interference.

**If you must run multiple brokers per server** (e.g. very large machines you want to fully utilize):
- Set **`broker.rack`** so that replica placement treats the **physical host** as the failure domain
  — replicas of a partition must **never** land on two brokers on the same server, or RF is a lie.
- Give each broker **dedicated NIC(s) and disks** (partition the hardware), not shared.
- Treat it as an optimization for huge servers only; for most fleets, **more smaller servers** beats
  fewer overloaded ones (blast radius, rebalance granularity, recovery time).

**KRaft note:** run **3 or 5 dedicated controller nodes** for production; co-locating controller +
broker roles is fine for small/dev clusters but not for large ones.

---

## How this maps to the course labs

- **Lab 1** — replication factor, ISR, `min.insync.replicas` (the durability primitives behind §1/§5).
- **Lab 5** — throughput/latency benchmarking, compression comparison, partition reassignment
  (use it to *measure* §1/§2 on your hardware).
- **Lab 9** — rolling upgrades, failover, and the failure modes that make the "one broker per failure
  domain" rule (§5) and the RF/min-ISR/`acks=all` config non-negotiable.
