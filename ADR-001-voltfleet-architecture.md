# ADR-001: VoltFleet EV Fleet Analytics Platform — Architecture Decisions

**Status:** Accepted  
**Date:** 2026-06-07  
**Author:** DT  
**Project:** VoltFleet — EV Fleet Predictive Analytics  

---

## Context

VoltFleet is a fleet analytics platform for a 500-vehicle commercial EV operator.
Each vehicle emits telemetry every 5 seconds: battery percentage, state of charge,
voltage, current draw, motor temperature, GPS coordinates, speed, and regenerative
braking events. That produces approximately 100 events per second at peak.

The platform must support:
- Real-time fleet health monitoring for operations staff
- Predictive maintenance alerts before failures occur, not after
- Clean historical data for data science model training
- No budget for enterprise or proprietary tooling — open source only

Each architecture decision below records the options considered, the tradeoffs
evaluated, and the decision taken, along with the business requirement that drove it.

---

## Decision Log

---

### ADR-001-01: Stream Processing Architecture — Lambda vs Kappa

**Options considered:**

- **Lambda architecture** (Nathan Marz, 2011): splits processing into a batch layer
  for historical accuracy and a speed layer for low-latency live data. A serving
  layer merges both outputs. The cost is maintaining two separate codebases that
  implement essentially the same logic.

- **Kappa architecture** (Jay Kreps, LinkedIn, 2014): a single streaming pipeline
  handles all data. Historical reprocessing is achieved by replaying the stream
  through the same pipeline rather than running a separate batch system.

**Tradeoffs:**

| Concern | Lambda | Kappa |
|---|---|---|
| Accuracy | High — batch layer corrects stream errors | High — if stream is reliable |
| Operational complexity | High — two systems to maintain | Lower — one pipeline |
| Reprocessing | Separate batch job | Replay the stream |
| Best suited to | Workloads where batch and stream logic diverge significantly | Workloads where stream processing is the primary model |

**Decision: Kappa**

At 100 events/second, VoltFleet is well within the range a single streaming pipeline
handles reliably. There is no requirement for complex batch reprocessing that would
justify the overhead of a second system. Kappa keeps the architecture maintainable.

**Consequence:** all historical queries and live analytics share the same pipeline
code. Reprocessing means replaying raw events through the same logic, not switching
to a separate batch system.

---

### ADR-001-02: Device Communication Protocol — MQTT vs HTTP/2

**Options considered:**

- **MQTT** (ISO/IEC 20922): a lightweight publish/subscribe protocol designed for
  constrained devices and unreliable networks. Built for low bandwidth, low power
  consumption, and graceful handling of intermittent connectivity. The standard
  choice for IoT devices on mobile networks.

- **HTTP/2**: heavier than MQTT but provides better error semantics, header
  compression, and request multiplexing. Suited to devices with reliable connections
  and more processing headroom.

**Tradeoffs:**

| Concern | MQTT | HTTP/2 |
|---|---|---|
| Network efficiency | Very high — minimal overhead | Moderate |
| Reliability on mobile networks | Excellent — built for it | Acceptable |
| Device power consumption | Low | Higher |
| Error handling | Basic (QoS levels 0/1/2) | Rich |
| Ecosystem complexity | Requires a broker (e.g. Mosquitto) | Standard web stack |

**Decision: MQTT for vehicles, HTTP for the simulated ingestion layer**

Commercial vans operate on 4G mobile networks with variable signal quality —
exactly the environment MQTT was designed for. In production the full stack would be:

```
EV (MQTT over 4G) → MQTT Broker (Mosquitto, open source) → Kafka → Stream Processor
```

For this implementation, vehicles are simulated in software on a local machine.
The simulator uses HTTP POST to the ingestion endpoint. The data contracts and
event structure are identical to what a real MQTT pipeline would produce, making
the transition to a real broker straightforward when required.

---

### ADR-001-03: Event Ingestion Engine — Kafka vs Kinesis vs Flink

**Options considered:**

- **Apache Kafka**: open source distributed event log. Producers write messages,
  consumers read them independently, messages are retained for a configurable period.
  Very high throughput. Runs on your own infrastructure. No licensing cost.

- **AWS Kinesis**: managed Kafka equivalent. Eliminates operational overhead but
  introduces AWS vendor lock-in and per-shard costs.

- **Apache Flink**: a stream processing engine that can also serve as an ingestion
  layer. True record-by-record processing at sub-second latency. Higher operational
  complexity than Kafka alone.

**Tradeoffs:**

| Concern | Kafka | Kinesis | Flink |
|---|---|---|---|
| Cost | Free (open source) | Per-shard pricing | Free (open source) |
| Vendor lock-in | None | AWS only | None |
| Operational overhead | Moderate (JVM cluster) | Low (managed) | High |
| Throughput | Very high | High | Very high |
| Replay capability | Yes — configurable retention | Yes | Depends on source |

**Decision: Kafka-shaped Python queue for this implementation**

Running a real Kafka cluster requires a JVM environment and is disproportionate for
a local development build. The implementation uses Python's `asyncio.Queue` as the
ingestion buffer — same producer/consumer mental model, same data contracts, no
cluster overhead.

The code is written to make Kafka substitution explicit: the queue interface mirrors
the Kafka producer/consumer API so the transition requires changing the transport
layer only, not the processing logic.

---

### ADR-001-04: Stream Processing Engine — Spark Streaming vs Flink

**Options considered:**

- **Apache Spark Streaming**: processes data in configurable micro-batches (e.g.
  every 30 seconds). Large ecosystem, mature ML library (MLlib), good integration
  with data lake formats.

- **Apache Flink**: true record-by-record stream processing at sub-second latency.
  Higher complexity. Better for applications where individual event latency matters.

**Tradeoffs:**

| Concern | Spark Streaming | Flink |
|---|---|---|
| Latency | Seconds (micro-batch) | Sub-second |
| ML integration | Excellent (MLlib) | Moderate |
| Operational complexity | Moderate | High |
| Best suited to | Analytics, ML pipelines, batch-compatible workloads | Real-time decisioning, event-driven triggers |

**Decision: Spark-style micro-batch processing implemented in Python/pandas**

Predictive maintenance does not require sub-second latency. Knowing a battery is
degrading 30 seconds after the signal appears is operationally acceptable. Spark's
micro-batch model is appropriate, and its ML ecosystem (which we mirror using
scikit-learn locally) is mature.

The implementation processes telemetry in 30-second windows using pandas — the same
logical operation Spark Streaming performs in a micro-batch. The window size is
configurable.

---

### ADR-001-05: Data Lake Organisation — Medallion Architecture

**Decision: Adopted — Bronze / Silver / Gold layers**

The medallion architecture (popularised by Databricks, widely adopted across the
industry) organises data lake storage into three progressive tiers:

| Layer | Contents | Purpose |
|---|---|---|
| Bronze | Raw, unmodified events as received | Source of truth. Never deleted. Enables full reprocessing. |
| Silver | Validated, cleaned, enriched records | Nulls removed, outliers flagged, units normalised, joined with vehicle metadata |
| Gold | Aggregated, business-ready data | Per-vehicle health scores, rolling averages, anomaly flags — ready for dashboards and models |

**Why this matters:** if the anomaly detection model produces incorrect predictions
and must be retrained, Bronze data makes full reprocessing possible. Without it,
historical recovery is not possible. Bronze is the audit trail.

**Implementation:** Bronze, Silver, and Gold are separate SQLite tables with a
clear naming convention. Gold layer analytical queries run through DuckDB.

---

### ADR-001-06: Storage Format — Apache Iceberg vs Delta Lake vs DuckDB

**Options considered:**

- **Apache Iceberg** (Netflix, Apache Foundation): adds ACID transactions, schema
  evolution, and time travel (point-in-time queries) on top of Parquet files stored
  in object storage (S3, GCS, ADLS). Works across multiple engines (Spark, Flink,
  Trino, DuckDB).

- **Delta Lake** (Databricks, open source): equivalent capabilities to Iceberg, with
  tighter Spark integration. Simpler to operate within the Databricks ecosystem.

- **DuckDB**: an in-process analytical database (OLAP-optimised, columnar storage).
  Comparable to SQLite in operational simplicity but designed for analytical
  workloads. Supports ACID transactions, can query Parquet files directly, and
  natively speaks Apache Arrow for zero-copy data transfer.

**Tradeoffs:**

| Concern | Iceberg | Delta Lake | DuckDB |
|---|---|---|---|
| ACID transactions | Yes | Yes | Yes |
| Time travel | Yes | Yes | Limited |
| Infrastructure required | Object storage + compute engine | Object storage + Spark | None — in-process |
| Production suitability | Large-scale | Large-scale | Small to medium scale |
| Local development | Complex to run locally | Complex to run locally | Trivial |

**Decision: DuckDB for the Gold analytical layer, SQLite for Bronze/Silver**

Both Iceberg and Delta Lake require distributed object storage and a compatible
compute engine to run. Neither is appropriate for a local implementation without
adding substantial infrastructure that would obscure the learning objectives.

DuckDB provides the analytical query performance, ACID compliance, and Arrow
compatibility of production-grade lake formats at zero operational cost. Several
production teams use DuckDB for analytics on datasets up to tens of gigabytes.

When this project scales to require distributed storage, the Gold layer queries
in DuckDB translate directly to the same SQL running against an Iceberg table.

---

### ADR-001-07: Zero Copy Architecture

**Context:** Zero copy is a data transfer pattern where bytes move between systems
without deserialisation and reserialisation. Apache Arrow defines a standard
in-memory columnar format that multiple systems (DuckDB, pandas 2.0+, Spark,
Polars) share without conversion cost.

**Decision: Use Arrow-native tools where available**

DuckDB natively reads and writes Apache Arrow. Pandas 2.0 uses Arrow as its
optional backend via `pandas.ArrowDtype`. This means data flowing from DuckDB into
pandas for model input does not need to be copied or converted — the same memory
region is read by both systems.

This is not explicitly architected as a feature but is a natural property of the
toolchain selected. It becomes significant at scale when processing millions of
records.

---

### ADR-001-08: ACID Compliance

**Requirement:** partial writes must never produce inconsistent state. A vehicle
record either commits fully or not at all.

**Decision: ACID-compliant storage throughout**

- **SQLite** (Bronze and Silver): fully ACID-compliant. The WAL (Write-Ahead Log)
  mode is enabled for improved concurrent read performance.
- **DuckDB** (Gold): fully ACID-compliant with serialisable isolation.

Note: raw Parquet files on object storage are not ACID-compliant without a
transaction log layer (which is what Iceberg and Delta Lake provide). This is a
known gap to address if the storage layer migrates to Parquet on object storage.

---

### ADR-001-09: Partitioning Strategy

**Context:** partitioning organises data so queries read only the relevant subset
rather than scanning the full dataset. In a production data lake this manifests as
folder structure: `/bronze/year=2026/month=06/vehicle_id=47/`.

**Decision: Index-based partitioning in SQLite, partition-aware schema design**

SQLite does not support filesystem-level partitioning but compound indexes on
`(vehicle_id, timestamp)` produce equivalent query plans — the engine reads only
rows matching the predicate.

The schema is designed to mirror production partitioning conventions. A migration to
Parquet + Iceberg would use `vehicle_id` and `date` as partition columns, matching
the index strategy already in place.

---

### ADR-001-10: Resilience Patterns — Circuit Breakers, Rate Limiting, Jitter

**Rate limiting**

The ingestion endpoint applies a token bucket rate limiter per vehicle ID. This
prevents a misbehaving vehicle (firmware bug, sensor fault) from flooding the
pipeline and affecting other vehicles. The token bucket algorithm is a standard
production pattern: each vehicle is allocated N tokens per second; each request
consumes one token; tokens replenish at a fixed rate.

**Jitter on retry**

When the ingestion service is unavailable and vehicles retry, simultaneous reconnection
by all 500 vehicles would create a thundering herd that would overwhelm the service
at the moment it recovers. Jitter adds a small random delay to each retry:

```
retry_delay = base_delay * (1 + random.uniform(0, 0.3))
```

This spreads reconnection attempts across a window rather than concentrating them.

**Circuit breaker (stub)**

A circuit breaker would monitor failure rates on the Silver processing step and,
above a configurable threshold, stop sending records to Silver and accumulate in
Bronze until the fault clears. This prevents a corrupted Silver processing run from
cascading into the Gold layer.

The implementation includes a clearly annotated stub showing where the circuit
breaker would sit. Full implementation is noted as a follow-on task.

---

### ADR-001-11: Dead Letter Queue

**Decision: Implemented as a `bronze_dlq` SQLite table**

Records that fail ingestion validation (malformed JSON, unknown vehicle ID, values
outside physically plausible ranges) are not silently dropped and not allowed to
block the pipeline. They are written to the DLQ with a timestamp, the raw payload,
and a structured error reason.

This is a standard production pattern. Silent drops make debugging impossible.
Blocking the pipeline on bad records is equally dangerous. The DLQ provides a
recoverable audit trail.

---

### ADR-001-12: Asynchronous Event-Driven Ingestion

**Decision: Asynchronous, non-blocking ingestion using Python asyncio**

Vehicles cannot block waiting for server acknowledgement. At 500 vehicles × 5-second
intervals, any synchronous blocking in the ingestion path would cause backpressure
that propagates back to the vehicles, causing missed readings and data gaps.

Python's `asyncio` library provides cooperative multitasking. The vehicle simulator
fires events asynchronously. The ingestion service processes them from the queue
without blocking producers. This mirrors the producer/consumer decoupling that Kafka
provides in production.

---

### ADR-001-13: Machine Learning — Anomaly Detection Model

**Algorithm: IsolationForest (scikit-learn)**

IsolationForest (Liu, Ting, Zhou — 2008) detects anomalies by randomly partitioning
the feature space. Anomalous records are isolated in fewer partitions than normal
records because they occupy sparse regions of the feature space. It is well-suited
to this problem because:

- No labelled failure data is required (unsupervised)
- It handles high-dimensional telemetry naturally
- Inference is fast enough for the 30-second micro-batch window

Features used: battery percentage, voltage, current draw, motor temperature,
rolling 5-minute battery discharge rate.

**Shadow mode**

A `shadow_predictions` table stores predictions from a candidate model running in
parallel with the production model. Shadow predictions are logged but do not trigger
alerts. This allows a new model to be validated on live data before promotion to
production, without risk to the operations team receiving false alerts.

---

## Summary of Technology Decisions

| Concern | Production choice | This implementation | Reason for substitution |
|---|---|---|---|
| Stream architecture | Kappa | Kappa | No substitution needed |
| Device protocol | MQTT (Mosquitto) | HTTP POST (simulated) | No physical devices |
| Event backbone | Apache Kafka | asyncio.Queue | No JVM cluster required locally |
| Stream processor | Apache Spark (micro-batch) | pandas + 30s window | No cluster required locally |
| Data organisation | Medallion (Bronze/Silver/Gold) | Medallion | No substitution needed |
| Lake format | Apache Iceberg | DuckDB + SQLite | No object storage required locally |
| Zero copy | Apache Arrow | DuckDB + pandas Arrow backend | Same toolchain |
| ACID storage | Iceberg transaction log | SQLite WAL + DuckDB | No object storage required locally |
| Partitioning | Parquet folder partitions | SQLite compound indexes | Same query logic |
| Rate limiting | API Gateway + token bucket | Python token bucket | No gateway required locally |
| Circuit breaker | Resilience4j / custom | Annotated stub | Complexity vs learning tradeoff |
| Jitter | Client-side retry logic | Python random.uniform | No substitution needed |
| DLQ | Kafka DLQ topic | SQLite bronze_dlq table | Same semantics, local storage |
| Async ingestion | Kafka producer/consumer | asyncio producer/consumer | Same mental model |
| Anomaly detection | IsolationForest | IsolationForest (scikit-learn) | No substitution needed |
| Shadow mode | Feature flag + log table | shadow_predictions table | No substitution needed |

---

## Constraints and Known Gaps

- **No distributed storage:** Bronze and Silver data lives in SQLite on a local
  filesystem. A production deployment would migrate to Parquet files on object
  storage (S3 or equivalent) with an Iceberg transaction log.

- **No real MQTT broker:** physical vehicles would connect via MQTT to a Mosquitto
  broker before events reach the ingestion service. The simulator bypasses this layer.

- **Circuit breaker is a stub:** the pattern is documented and positioned in the
  code but not fully implemented in this sprint.

- **Single-node only:** no horizontal scaling. A production deployment would run
  the ingestion service behind a load balancer with multiple instances.

- **No authentication:** vehicles are identified by ID only. A production system
  would require mutual TLS (mTLS) between vehicles and the ingestion endpoint.

---

*This ADR should be updated when any decision is revisited or when the implementation
migrates from local simulation to production infrastructure.*
