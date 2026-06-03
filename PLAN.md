# Course Build Plan — Advanced Kafka with Streaming Architecture

**Status:** working plan. Verify existing labs first, then build the new content below.
**Delivery date:** 2026-07-06 (4 days × 8h, client Cognixia) — ~5 weeks of lead time.
**Last updated:** 2026-06-03

---

## Why this plan exists

The course is contracted as **4 days × 8 hours = 32 contact hours**, lecture/lab ~50/50
(`outline.md` → Delivery Model). The current material does **not** fill that:

- 7 decks, ~19 slides each → **136 module slides** (+11 intro)
- 7 labs, 60–90 min each

Estimated content: **~18 h lean / ~23.5 h generous** against a **32 h** container —
roughly **3 days of a 4-day slot**. As structured (2 modules/day), all 7 modules finish
by ~lunch on Day 3; **Day 4 is nearly empty**. The plan below adds ~8–12 h of scoped
content (3 labs + a capstone + deeper decks) to close the gap.

---

## Revised 4-day schedule

Lecture estimated at ~4 min/slide; labs at their stated midpoints. ⭐NEW = must be built.
Each day's content sums to ~7.5 h; breaks + lunch bring it to 8 h.

### Day 1 — Foundations & Internals
| Block | Type | Time |
|---|---|---|
| Welcome, intro, environment tour (`about`) | Lecture | 0.5 h |
| M1 — Modern EDA *(deepen to ~26 slides)* | Lecture | 1.75 h |
| Lab 1 — Cluster topology & topics | Lab | 1.25 h |
| M2 — Kafka Internals & Architecture *(deepen)* | Lecture | 1.75 h |
| Lab 2 — Internals | Lab | 1.25 h |
| Design discussion + Day-1 wrap | Disc | 0.75 h |
| **Total** | | **7.25 h** |

### Day 2 — Operations & Integration
| Block | Type | Time |
|---|---|---|
| Recap | — | 0.25 h |
| M3 — Operations & Observability | Lecture | 1.25 h |
| Lab 3 — Operations | Lab | 1.25 h |
| ⭐NEW Lab — Monitoring (Prometheus/Grafana) | Lab | 1.0 h |
| M4 — Connectors, Pipelines & Integrations | Lecture | 1.25 h |
| Lab 4 — Connectors | Lab | 1.0 h |
| ⭐NEW Lab — Schema Registry (Avro/Protobuf) | Lab | 1.0 h |
| Wrap | — | 0.25 h |
| **Total** | | **7.25 h** |

### Day 3 — Reliability, Scale & Modern Streaming
| Block | Type | Time |
|---|---|---|
| Recap | — | 0.25 h |
| M5 — Reliability, Scaling & Performance *(deepen to ~26)* | Lecture | 1.75 h |
| Lab 5 — Reliability | Lab | 1.5 h |
| M6 — Modern Kafka & Streaming Trends | Lecture | 1.25 h |
| Lab 6 — Modern trends | Lab | 1.5 h |
| ⭐NEW Lab — Flink SQL stream processing | Lab | 1.0 h |
| Wrap | — | 0.25 h |
| **Total** | | **7.5 h** |

### Day 4 — Fan-Out & Capstone
| Block | Type | Time |
|---|---|---|
| Recap | — | 0.25 h |
| M7 — High-Volume Fan-Out *(deepen to ~26)* | Lecture | 1.75 h |
| Lab 7 — Fan-out | Lab | 1.25 h |
| ⭐NEW Capstone brief — end-to-end pipeline | Lecture | 0.25 h |
| ⭐NEW Capstone build — Connect → Kafka → Flink → sink + monitoring | Lab | 2.5 h |
| Capstone presentations + review | Disc | 0.75 h |
| Course wrap, Q&A, next steps | — | 0.5 h |
| **Total** | | **7.25 h** |

### Totals
- ~29.25 h content + ~2.75 h breaks/lunch ≈ **32 h** ✅
- Lecture ~13 h / Hands-on ~16 h → ~45/55 (matches outline's ~50/50, slightly lab-heavy)
- Day 4 hole filled by the capstone, which also ties the course together

---

## Build checklist (to make the schedule real)

- [ ] **⭐ New Lab — Monitoring (Prometheus + Grafana)** — broker/JMX metrics, dashboards, lag alerts. Pairs with M3.
- [ ] **⭐ New Lab — Schema Registry (Avro/Protobuf)** — register schema, produce/consume with serdes, compatibility/evolution. Pairs with M4.
- [ ] **⭐ New Lab — Flink SQL** — continuous SQL over a Kafka topic; `outline.md` lists Flink as a new addition. Pairs with M6.
- [ ] **⭐ Capstone** — end-to-end pipeline (Connect → Kafka → Flink/Streams → sink) with monitoring; spec + starter scaffolding + grading rubric. Day 4.
- [ ] **Deepen decks** — M5, M7 (and M1) from ~17–22 → ~26 slides: diagrams, real-world failure stories.
- [ ] **Extend `docker-compose.yml`** with the deferred profiles the new labs need:
  - [ ] `connect` — Kafka Connect
  - [ ] `monitoring` — Prometheus + Grafana
  - [ ] `flink` — Apache Flink
  - [ ] Schema Registry service

---

## Open decisions / inconsistencies to reconcile

- **UI tool: Kafdrop vs Kafka UI.** `outline.md` (System Requirements) says **Kafdrop + Prometheus + Grafana**; the labs and `docker-compose.yml` use **Kafka UI** (kafbat). Pick one as canonical. (`module1.md` was already edited to drop a stray "Kafdrop" mention so it matched the labs — revisit if Kafdrop becomes canonical.)
- **Lab environment parity.** Labs run on local Docker Compose; the main course env is **Strimzi on Kubernetes** (`SETUP.md`). New labs must keep the "same `kafka-*.sh` command via `kubectl exec`" promise.

---

## Sequencing

1. **Verify existing 7 labs** on the current Compose cluster (in progress: Lab 1) — extend a known-good base, not unverified one.
2. Build the **compose profiles** (connect / monitoring / flink / schema-registry).
3. Author the **3 new labs**, then the **capstone**.
4. **Deepen** the thin decks (M5, M7, M1).
5. Reconcile the **Kafdrop vs Kafka UI** decision while building the monitoring lab.