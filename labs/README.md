# Advanced Kafka with Streaming Architecture — Labs

This directory contains all 7 hands-on lab guides, one per module.

> **⚠️ Old folders still present — ignore them.**
> The course was restructured from 10 modules to 7 modules to match the client outline.
> Old folders (`03-Topic-Design`, `04-Stream-Processing`, `05-Connectors`, `06-Reliability`, `07-Security`, `08-Observability`, `09-Trends`, `10-Capstone`) each contain a `SUPERSEDED.md` explaining what replaced them.
> **Only use the folders listed in the Active Lab Index below.**

## Active Lab Index

| # | Module | Lab file | Duration |
|---|--------|----------|----------|
| 1 | Modern Event-Driven Architecture with Kafka | `01-Modern-EDA/lab-01-kafka-topology.md` | 45–60 min |
| 2 | Kafka Internals & Cluster Architecture | `02-Kafka-Internals/lab-02-kafka-internals.md` | 45–60 min |
| 3 | Kafka Operations & Observability | `03-Operations-Observability/lab-03-operations-observability.md` | 60–75 min |
| 4 | Connectors, Pipelines & Integrations | `04-Connectors/lab-04-connectors.md` | 60–75 min |
| 5 | Reliability, Scaling & Performance | `05-Reliability/lab-05-reliability.md` | 75–90 min |
| 6 | Modern Kafka & Streaming Trends | `06-Modern-Trends/lab-06-modern-trends.md` | 60 min |
| 7 | High-Volume Fan-Out Best Practices | `07-Fan-Out/lab-07-fan-out.md` | 75–90 min |

## Notes

- Labs are written as standalone markdown and can be converted to slides or Jupyter notebooks.
- All commands are copyable and designed for the provided Docker Compose lab environment.
- Use `SETUP.md` and `QUICKSTART.md` for environment preparation before starting Lab 1.
- Each lab builds on the running cluster from previous labs — do not tear down the environment between modules unless instructed.

## Day Schedule (suggested)

| Day | Modules | Labs |
|-----|---------|------|
| Day 1 | 1, 2 | Lab 1, Lab 2 |
| Day 2 | 3, 4 | Lab 3, Lab 4 |
| Day 3 | 5 | Lab 5 |
| Day 4 | 6, 7 | Lab 6, Lab 7 |
