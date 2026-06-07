# PRD-001: VoltFleet — EV Fleet Predictive Analytics Platform

**Status:** Draft  
**Version:** 1.0  
**Date:** 2026-06-07  
**Author:** DT  
**Related documents:** ADR-001-voltfleet-architecture.md  

---

## Table of Contents

1. [Purpose and Scope](#1-purpose-and-scope)
2. [Stakeholders](#2-stakeholders)
3. [Business Requirements](#3-business-requirements)
4. [Functional Requirements](#4-functional-requirements)
5. [Non-Functional Requirements](#5-non-functional-requirements)
6. [Operational Requirements](#6-operational-requirements)
7. [Data Privacy and Regulatory Compliance](#7-data-privacy-and-regulatory-compliance)
8. [Security Requirements](#8-security-requirements)
9. [Resilience and Availability Requirements](#9-resilience-and-availability-requirements)
10. [Latency Requirements](#10-latency-requirements)
11. [Geographic and Infrastructure Requirements](#11-geographic-and-infrastructure-requirements)
12. [Out of Scope](#12-out-of-scope)
13. [Acceptance Criteria](#13-acceptance-criteria)
14. [Implementation Phases](#14-implementation-phases)
15. [Glossary](#15-glossary)

---

## 1. Purpose and Scope

### 1.1 Problem Statement

A commercial logistics operator running a fleet of 500 electric vans across
Europe, the USA/Canada, and Japan currently has no visibility into battery
health trends or early warning signals for mechanical degradation. Vehicles
fail in the field without prior indication. Unplanned downtime costs the
operator an estimated 4-6 hours of recovery time per incident, plus roadside
assistance and potential cargo delays.

### 1.2 Proposed Solution

VoltFleet is a telemetry ingestion and analytics platform that:

- Continuously collects vehicle sensor data (battery, thermal, electrical, GPS)
- Stores data in a structured medallion architecture (Bronze/Silver/Gold)
- Detects anomalous patterns using an unsupervised ML model (IsolationForest)
- Surfaces predictive maintenance alerts before failures occur
- Provides a fleet health dashboard for operations staff

### 1.3 Scope of This Document

This PRD covers the first production-ready version (v1.0) of VoltFleet. It
defines what the system must do, how it must behave under load, what privacy
and security obligations it must satisfy, and what infrastructure it requires
across the three operating markets.

### 1.4 What This Is Not

VoltFleet is not a vehicle tracking or route optimisation system. It is not
a driver performance tool. It is not connected to the vehicles' control
systems and cannot issue any commands to a vehicle.

---

## 2. Stakeholders

| Role | Interest in the system |
|---|---|
| Fleet Operations Manager | Live dashboard: which vehicles need attention right now |
| Maintenance Team | Predictive alerts: which vehicles to inspect before failure |
| Data Science Team | Clean, partitioned historical data for model training |
| Information Security Officer | Compliance, data residency, access controls |
| Legal / DPO (Data Protection Officer) | GDPR, US state privacy laws, APPI compliance |
| Platform Engineering | System reliability, observability, deployment |

---

## 3. Business Requirements

| ID | Requirement |
|---|---|
| BR-001 | The system shall reduce unplanned vehicle downtime by providing predictive alerts at least 24 hours before a probable failure event |
| BR-002 | The system shall operate across fleet vehicles in the UK/EU, USA/Canada, and Japan without requiring separate deployments for each region |
| BR-003 | All vehicle data shall be retained for a minimum of 3 years to satisfy audit and warranty claim requirements |
| BR-004 | The system shall use only open-source or zero-cost components; no proprietary or subscription-based tooling |
| BR-005 | The system shall be operable by a small engineering team without specialised data engineering headcount |

---

## 4. Functional Requirements

### 4.1 Vehicle Telemetry Ingestion

| ID | Requirement |
|---|---|
| FR-001 | The ingestion service shall accept telemetry events from up to 500 concurrent vehicles |
| FR-002 | Each event shall contain: vehicle_id, timestamp (UTC ISO 8601), battery_pct, state_of_charge_kwh, voltage_v, current_a, motor_temp_c, latitude, longitude, speed_kmh, regen_braking_event (boolean) |
| FR-003 | The ingestion service shall validate all incoming events against a defined schema before accepting them |
| FR-004 | Events failing validation shall be written to the Dead Letter Queue (DLQ) with a structured error reason; they shall not be silently dropped |
| FR-005 | The ingestion service shall apply a per-vehicle rate limit of 1 event per 4 seconds; excess events shall be rejected with HTTP 429 |
| FR-006 | The ingestion service shall accept events asynchronously; vehicle clients shall not block waiting for processing confirmation |

### 4.2 Data Pipeline

| ID | Requirement |
|---|---|
| FR-007 | Raw events shall be written to the Bronze layer without modification within 2 seconds of receipt |
| FR-008 | A pipeline job shall run every 30 seconds, processing Bronze records into the Silver layer (validation, normalisation, enrichment with vehicle metadata) |
| FR-009 | A pipeline job shall run every 30 seconds, aggregating Silver records into the Gold layer (rolling averages, health scores, anomaly flags) |
| FR-010 | The Bronze layer shall never be modified or deleted after initial write |
| FR-011 | Pipeline failures shall not cause data loss; unprocessed Bronze records shall remain available for reprocessing |

### 4.3 Anomaly Detection

| ID | Requirement |
|---|---|
| FR-012 | The anomaly detection model shall evaluate each vehicle's Gold layer record every 30 seconds |
| FR-013 | The model shall flag a vehicle as anomalous when its feature vector deviates significantly from the fleet baseline (IsolationForest contamination parameter: 0.05) |
| FR-014 | Features used for anomaly detection shall include: battery_pct, voltage_v, current_a, motor_temp_c, rolling_5min_discharge_rate |
| FR-015 | Each model prediction shall be written to a predictions table with: vehicle_id, timestamp, anomaly_score, is_anomaly, model_version, mode (production / shadow) |
| FR-016 | A shadow mode model shall run in parallel with the production model; shadow predictions shall be logged but shall not trigger alerts |
| FR-017 | The production model shall be retrained on a rolling 30-day window of Silver data no less than once per week |

### 4.4 Alerting

| ID | Requirement |
|---|---|
| FR-018 | When a vehicle is flagged anomalous in two consecutive 30-second evaluation windows, an alert shall be raised |
| FR-019 | Alerts shall include: vehicle_id, first_detected timestamp, current anomaly score, top contributing features |
| FR-020 | Alerts shall be stored in an alerts table and surfaced on the dashboard |
| FR-021 | Alert fatigue mitigation: once an alert is raised for a vehicle, no further alerts for that vehicle shall be raised for 4 hours unless the anomaly score increases by more than 20% |

### 4.5 Dashboard

| ID | Requirement |
|---|---|
| FR-022 | The dashboard shall show a live fleet summary: total vehicles, vehicles online, vehicles with active alerts, average fleet battery percentage |
| FR-023 | The dashboard shall show a per-vehicle detail view: last 60 minutes of telemetry for selected vehicle, current anomaly score, alert history |
| FR-024 | The dashboard shall refresh automatically every 30 seconds without requiring a page reload |
| FR-025 | The dashboard shall be accessible via a web browser; no client-side installation required |

---

## 5. Non-Functional Requirements

| ID | Requirement |
|---|---|
| NFR-001 | The system shall process 100 telemetry events per second under normal load without queuing delay exceeding 2 seconds |
| NFR-002 | The system shall handle burst load of 300 events per second for up to 60 seconds without data loss |
| NFR-003 | Dashboard page load shall complete within 3 seconds on a standard broadband connection |
| NFR-004 | The anomaly detection pipeline shall complete a full fleet evaluation cycle within 30 seconds |
| NFR-005 | The system shall be observable: all pipeline stages shall emit structured logs with stage name, record count, processing time, and error count |
| NFR-006 | All configuration (thresholds, window sizes, model parameters) shall be externalised to a config file; no hardcoded values in application code |
| NFR-007 | All code shall be annotated; a new engineer should be able to understand any function's purpose without referring to external documentation |

---

## 6. Operational Requirements

### 6.1 Logging

| ID | Requirement |
|---|---|
| OR-001 | All services shall emit structured JSON logs (not freeform text) |
| OR-002 | Log entries shall include: timestamp (UTC), service_name, log_level, message, correlation_id where applicable |
| OR-003 | Log levels shall be configurable at runtime without redeployment |
| OR-004 | Logs shall be retained for a minimum of 90 days |

### 6.2 Monitoring and Alerting

| ID | Requirement |
|---|---|
| OR-005 | The system shall expose a /health endpoint returning current status of each pipeline component |
| OR-006 | The system shall expose a /metrics endpoint with: events ingested per minute, DLQ depth, Silver processing lag, Gold processing lag, active alert count |
| OR-007 | An operational alert shall fire if the DLQ depth exceeds 100 records, indicating a systematic ingestion problem |
| OR-008 | An operational alert shall fire if the Silver or Gold pipeline has not run successfully within 90 seconds (3× the expected interval) |

### 6.3 Deployment

| ID | Requirement |
|---|---|
| OR-009 | The full system shall be deployable via a single command on a fresh machine using Docker Compose |
| OR-010 | Environment-specific configuration (database paths, API keys, regional settings) shall be managed via environment variables, never committed to version control |
| OR-011 | A seed script shall be provided to initialise the database schema and load vehicle metadata fixtures |
| OR-012 | The system shall support zero-downtime restarts of the ingestion service; in-flight events shall be drained before shutdown |

---

## 7. Data Privacy and Regulatory Compliance

VoltFleet processes telemetry data that includes precise GPS coordinates tied to a
vehicle identifier. Depending on jurisdiction, this may constitute personal data if
the vehicle can be linked to an individual (e.g. an assigned driver). The system
must satisfy obligations in all three operating markets simultaneously.

### 7.1 Data Classification

| Data type | Classification | Rationale |
|---|---|---|
| vehicle_id | Pseudonymous personal data | Can be linked to a named driver via the operator's HR system |
| GPS coordinates + timestamp | Personal data (where driver-assigned) | Constitutes location history of an individual |
| Battery, voltage, thermal readings | Operational data | No direct personal data unless linked to vehicle_id |
| Aggregated fleet statistics | Non-personal | No individual identifiable |

### 7.2 Europe — GDPR (Regulation (EU) 2016/679)

| ID | Requirement |
|---|---|
| PRIV-EU-001 | A lawful basis for processing shall be documented before data collection begins. For fleet operations, the likely basis is Article 6(1)(f) — legitimate interests. A Legitimate Interests Assessment (LIA) shall be completed and retained |
| PRIV-EU-002 | Drivers shall be informed of what data is collected, for what purpose, and how long it is retained, via a privacy notice meeting Article 13 requirements |
| PRIV-EU-003 | GPS and vehicle_id data shall not be stored in raw form beyond the Bronze retention window without pseudonymisation. In the Silver layer, vehicle_id shall be stored as a one-way hash for analytics purposes; the mapping table shall be held separately with restricted access |
| PRIV-EU-004 | Data shall not be transferred outside the European Economic Area unless an adequacy decision or appropriate safeguard (Standard Contractual Clauses) is in place. Japan holds an adequacy decision (Commission Decision 2019/419). The USA does not hold a blanket adequacy decision; transfers to US infrastructure require SCCs or use of the EU-US Data Privacy Framework |
| PRIV-EU-005 | The system shall support the right to erasure (Article 17): a process shall exist to remove all records linked to a specific vehicle_id across Bronze, Silver, Gold, and predictions tables upon a valid request. Note: Bronze records are immutable by design; the process shall overwrite the vehicle_id field with a null token and log the erasure event |
| PRIV-EU-006 | A Data Protection Impact Assessment (DPIA) shall be completed before go-live, given the systematic monitoring of individuals' location (Article 35) |
| PRIV-EU-007 | Data retention: raw telemetry (Bronze) shall be deleted after 3 years. Silver and Gold records shall be deleted after 5 years. Retention schedules shall be enforced by an automated job, not manual process |

### 7.3 USA and Canada

**USA — no single federal privacy law applies.** The relevant instruments are
sector-specific and state-level.

| ID | Requirement |
|---|---|
| PRIV-US-001 | California Consumer Privacy Act (CCPA) / CPRA: if any California residents are drivers, the operator is likely a "business" under CCPA. Drivers shall have the right to know what data is collected, the right to delete, and the right to opt out of sale. VoltFleet does not sell data, but this shall be documented |
| PRIV-US-002 | State law patchwork: Virginia (VCDPA), Colorado (CPA), Connecticut (CTDPA), and several others have equivalent frameworks to CCPA. The system shall treat all US driver data to CCPA standard as a baseline, which satisfies the majority of state requirements |
| PRIV-US-003 | Employee monitoring: several US states (Connecticut, Delaware, New York) require employers to notify employees of electronic monitoring. A written notice policy shall be in place before deployment |
| PRIV-US-004 | Canada — PIPEDA (Personal Information Protection and Electronic Documents Act) / Bill C-27 (in progress): consent or a legitimate purpose is required for collection of location data. Drivers shall be notified and a privacy policy shall be published |
| PRIV-US-005 | Data residency: US and Canadian vehicle data shall be stored on infrastructure physically located in North America. It shall not be routed through European data centres without SCCs in place |

### 7.4 Japan — APPI (Act on the Protection of Personal Information, 2022 revision)

| ID | Requirement |
|---|---|
| PRIV-JP-001 | GPS + vehicle_id constitutes "personal information" under APPI. A purpose of use shall be specified and notified to data subjects before or at the point of collection |
| PRIV-JP-002 | Data shall not be provided to third parties without prior consent, unless a statutory exemption applies |
| PRIV-JP-003 | Japan vehicle data shall be stored on infrastructure physically located in Japan or in a country with equivalent protection standards. Japan holds an EU adequacy decision, making EU↔Japan transfers permissible; US↔Japan transfers require contractual safeguards |
| PRIV-JP-004 | In the event of a data breach affecting personal information, the Personal Information Protection Commission (PPC) shall be notified within a practicable timeframe (the 2022 revision introduced mandatory breach notification) |
| PRIV-JP-005 | Anonymised information (where re-identification is not possible) is not subject to APPI restrictions. The Gold layer aggregated statistics qualify if vehicle_id is removed and statistical k-anonymity (k≥3) is maintained |

### 7.5 Cross-Market Requirements

| ID | Requirement |
|---|---|
| PRIV-X-001 | A single Data Retention Policy document shall govern all three markets, noting where market-specific obligations differ |
| PRIV-X-002 | Access to raw Bronze data (which contains unmasked vehicle_id and GPS) shall be restricted to named individuals with a documented business need. Access shall be logged |
| PRIV-X-003 | All data in transit shall be encrypted (TLS 1.2 minimum, TLS 1.3 preferred) |
| PRIV-X-004 | All data at rest shall be encrypted (AES-256) |
| PRIV-X-005 | A breach response procedure shall be documented before go-live, covering notification timelines for each jurisdiction: GDPR (72 hours to supervisory authority), APPI (practicable timeframe to PPC), US (varies by state; 30–90 days is common) |

---

## 8. Security Requirements

### 8.1 Authentication and Authorisation

| ID | Requirement |
|---|---|
| SEC-001 | The ingestion endpoint shall authenticate vehicles using a per-vehicle API key. Keys shall be rotatable without service downtime |
| SEC-002 | In production, vehicle-to-service communication shall use mutual TLS (mTLS); the vehicle presents a certificate, the service validates it |
| SEC-003 | The dashboard shall require user authentication. In v1.0, HTTP Basic Auth over TLS is acceptable; v1.1 shall implement OAuth 2.0 / OIDC |
| SEC-004 | Role-based access control shall be defined: Operations (read-only dashboard), Maintenance (read alerts + write acknowledgements), Engineering (full access), Admin (user management) |
| SEC-005 | API keys and secrets shall never appear in logs, error messages, or HTTP responses |

### 8.2 Input Validation and Injection Prevention

| ID | Requirement |
|---|---|
| SEC-006 | All telemetry fields shall be validated for type, range, and format before being written to any storage layer |
| SEC-007 | No raw user input or telemetry field value shall be interpolated directly into a SQL query; parameterised queries shall be used throughout |
| SEC-008 | The ingestion endpoint shall enforce a maximum payload size of 4KB per event; larger payloads shall be rejected with HTTP 413 |

### 8.3 Infrastructure Security

| ID | Requirement |
|---|---|
| SEC-009 | No service shall run as root. All processes shall run under a dedicated low-privilege service account |
| SEC-010 | Database files shall have filesystem permissions restricted to the service account only (chmod 600) |
| SEC-011 | Dependency versions shall be pinned in requirements.txt; a dependency audit (pip-audit) shall be run before each release |
| SEC-012 | The /health and /metrics endpoints shall not be publicly accessible; they shall be restricted to the internal network or an authenticated monitoring agent |

---

## 9. Resilience and Availability Requirements

### 9.1 Target Availability

| Market | Target availability | Rationale |
|---|---|---|
| EU (UK/Europe) | 99.5% monthly | Fleet ops during business hours; overnight downtime tolerable |
| USA / Canada | 99.5% monthly | As above; time zones mean some overlap with EU maintenance windows |
| Japan | 99.5% monthly | As above |
| Combined (any market affected) | 99.0% monthly | Shared services layer |

99.5% monthly permits approximately 3.6 hours of downtime per month.
This is achievable without active-active redundancy.

### 9.2 Failure Mode Handling

| Failure | Expected behaviour |
|---|---|
| Ingestion service restart | In-flight events drained before shutdown (graceful shutdown). Vehicle clients retry with jitter. No data loss if retry window < Bronze retention period |
| Bronze pipeline failure | Silver and Gold processing stops. Bronze accumulates. On recovery, pipeline reprocesses from the last successful Silver watermark. No data loss |
| Silver pipeline failure | Gold processing stops. Silver accumulates. Bronze unaffected. On recovery, Gold reprocesses from last watermark |
| Database write failure | Event routed to DLQ. Ingestion continues. DLQ depth metric triggers operational alert |
| Anomaly model failure | Alerting stops. Dashboard shows stale scores with a warning indicator. No data loss. Model restarts independently of ingestion |
| Dashboard failure | Data collection and alerting unaffected. Operations team falls back to direct database query |

### 9.3 Data Durability

| ID | Requirement |
|---|---|
| RES-001 | Bronze data shall be written with SQLite WAL mode enabled, ensuring durability on process crash |
| RES-002 | A daily backup of the Bronze database shall be written to a separate filesystem path (or object storage in production) |
| RES-003 | Backup restoration shall be tested and documented; an untested backup is not a backup |

### 9.4 Circuit Breaker

| ID | Requirement |
|---|---|
| RES-004 | A circuit breaker shall protect the Silver processing stage: if more than 10% of records in a batch fail validation, processing shall halt and raise an operational alert rather than writing a corrupted batch to Silver |
| RES-005 | The circuit breaker state (closed / open / half-open) shall be visible on the /health endpoint |

---

## 10. Latency Requirements

| Pipeline stage | Maximum acceptable latency | Rationale |
|---|---|---|
| Vehicle event → Bronze write | 2 seconds | Near-real-time ingestion; vehicle should not wait |
| Bronze → Silver processing | 30 seconds (micro-batch interval) | Operational visibility; 30s lag is acceptable |
| Silver → Gold aggregation | 30 seconds (micro-batch interval) | Dashboard acceptable staleness |
| Gold → Anomaly evaluation | 30 seconds | Predictive alerts; minute-level granularity is sufficient |
| Anomaly alert → Dashboard visible | 5 seconds | Alert must appear promptly once raised |
| Dashboard auto-refresh | 30 seconds | Consistent with pipeline cadence |

**End-to-end worst case:** a real-world event takes up to 97 seconds from vehicle
emission to appearing as an anomaly alert on the dashboard (2s ingestion +
30s Bronze→Silver + 30s Silver→Gold + 30s Gold→model + 5s alert render).
This is acceptable for the predictive maintenance use case, which operates on
trends over minutes, not individual events.

### 10.1 Latency Under Load

| ID | Requirement |
|---|---|
| LAT-001 | At 100 events/second (normal load), Bronze write latency shall not exceed 2 seconds at the 95th percentile |
| LAT-002 | At 300 events/second (burst load, 60-second window), Bronze write latency shall not exceed 5 seconds at the 95th percentile. Events shall not be dropped; they shall queue |
| LAT-003 | Dashboard page load (initial) shall complete within 3 seconds on a 20Mbps connection |
| LAT-004 | Dashboard auto-refresh (incremental data fetch) shall complete within 1 second |

---

## 11. Geographic and Infrastructure Requirements

### 11.1 Data Residency Architecture

VoltFleet must comply with data residency requirements across three markets.
The architecture uses regional deployments sharing a common application codebase
but with independent storage layers.

```
┌─────────────────────────────────────────────────────────────────┐
│                     GLOBAL CONTROL PLANE                        │
│         (Model registry, shared config, dashboard auth)         │
│                    Hosted: EU (primary)                         │
└────────────┬──────────────────────┬───────────────────┬─────────┘
             │                      │                   │
             ▼                      ▼                   ▼
┌────────────────────┐  ┌────────────────────┐  ┌──────────────────┐
│   EU REGION        │  │  NA REGION         │  │  JAPAN REGION    │
│   (e.g. AWS        │  │  (e.g. AWS         │  │  (e.g. AWS       │
│   eu-west-1)       │  │  us-east-1)        │  │  ap-northeast-1) │
│                    │  │                    │  │                  │
│  Ingestion svc     │  │  Ingestion svc     │  │  Ingestion svc   │
│  Bronze (SQLite/S3)│  │  Bronze (SQLite/S3)│  │  Bronze          │
│  Silver            │  │  Silver            │  │  Silver          │
│  Gold              │  │  Gold              │  │  Gold            │
│  Anomaly model     │  │  Anomaly model     │  │  Anomaly model   │
│  Dashboard         │  │  Dashboard         │  │  Dashboard       │
└────────────────────┘  └────────────────────┘  └──────────────────┘
        │                        │                       │
        └────────────────────────┴───────────────────────┘
                          Fleet-level Gold
                     aggregation (anonymised only,
                      EU hosted, no personal data)
```

Vehicle data never leaves its region of origin. Only anonymised, aggregated
Gold statistics (no vehicle_id, no GPS, k-anonymity maintained) are replicated
to the global control plane for fleet-level reporting.

### 11.2 Load Balancing

| ID | Requirement |
|---|---|
| INFRA-001 | Each regional ingestion service shall sit behind a load balancer with health-check-based routing. If an ingestion instance fails its health check, the load balancer removes it from rotation without manual intervention |
| INFRA-002 | The load balancer shall perform TLS termination; application services communicate internally over HTTP within the private network |
| INFRA-003 | In v1.0 (local/single-node deployment), the load balancer is represented as a configuration stub. It shall be implemented before any production deployment |

### 11.3 DNS and Routing

| ID | Requirement |
|---|---|
| INFRA-004 | Each region shall be addressable via a regional subdomain: eu.voltfleet.internal, na.voltfleet.internal, jp.voltfleet.internal |
| INFRA-005 | Vehicles shall be pre-configured with their regional endpoint at provisioning time. Cross-region fallback routing is not required in v1.0 |

### 11.4 This Implementation (Local/Single Node)

The v1.0 implementation runs on a single machine and simulates the regional
architecture through configuration rather than physical separation. Environment
variables control which "region" is active. The data models, schemas, and privacy
handling are identical to a production regional deployment.

---

## 12. Out of Scope

The following are explicitly not in scope for v1.0:

- Route optimisation or journey planning
- Driver performance scoring or monitoring
- Vehicle remote control or over-the-air firmware updates
- Integration with third-party telematics providers
- Mobile application
- Real MQTT broker (Mosquitto) — simulated in v1.0
- Real Kafka cluster — simulated in v1.0
- Real object storage (S3/GCS) — SQLite used in v1.0
- Horizontal scaling / multi-instance deployment — single node in v1.0
- Full OAuth 2.0 / OIDC authentication — deferred to v1.1
- Automated DR failover — manual process in v1.0

---

## 13. Acceptance Criteria

The system is considered complete when all of the following are demonstrable:

| ID | Criterion |
|---|---|
| AC-001 | 10 simulated vehicles emit telemetry for 5 minutes; all events appear in the Bronze table without loss |
| AC-002 | A deliberately injected anomalous vehicle (battery dropping at 3× normal rate) triggers an alert within 3 pipeline cycles (90 seconds) |
| AC-003 | A malformed event (missing required field) appears in the DLQ table with a structured error reason, not in Bronze |
| AC-004 | The /health endpoint returns the status of all pipeline components |
| AC-005 | Shadow mode predictions are logged separately and do not trigger alerts |
| AC-006 | Erasing a vehicle_id from all tables via the erasure script produces a confirmed audit log entry |
| AC-007 | All pytest tests pass with no failures |
| AC-008 | The README documents how to run the full system from a clean clone |

---

## 14. Implementation Phases

### Phase 1 — Core Pipeline (Weekend Day 1, morning)
- Project structure and environment setup
- Vehicle simulator (async, jitter, configurable vehicle count)
- Ingestion service (rate limiter, schema validation, DLQ)
- Bronze layer write

### Phase 2 — Medallion Pipeline (Weekend Day 1, afternoon)
- Silver processing (validation, normalisation, vehicle metadata join)
- Gold aggregation (rolling averages, discharge rate, health score)
- Pipeline scheduler (30-second micro-batch loop)

### Phase 3 — Anomaly Detection (Weekend Day 2, morning)
- IsolationForest model training on Gold features
- Prediction writer (production + shadow mode)
- Alert logic (consecutive anomaly windows, fatigue mitigation)

### Phase 4 — Observability and Compliance (Weekend Day 2, afternoon)
- /health and /metrics endpoints
- Structured logging throughout
- Vehicle erasure script (GDPR/APPI right to erasure)
- Pytest suite covering all acceptance criteria

### Phase 5 — Stretch Goal
- Flask dashboard (fleet summary, per-vehicle detail, auto-refresh)

---

## 15. Glossary

| Term | Definition |
|---|---|
| ACID | Atomicity, Consistency, Isolation, Durability — properties of a reliable database transaction |
| APPI | Act on the Protection of Personal Information — Japan's primary data privacy law |
| Bronze | The raw, unmodified data layer in a medallion architecture |
| CCPA | California Consumer Privacy Act |
| Circuit breaker | A resilience pattern that halts processing when failure rates exceed a threshold, preventing cascading failures |
| DLQ | Dead Letter Queue — a store for events that failed processing, for investigation and reprocessing |
| DPIA | Data Protection Impact Assessment — a GDPR requirement for high-risk processing activities |
| DPO | Data Protection Officer |
| GDPR | General Data Protection Regulation (EU) 2016/679 |
| Gold | The aggregated, business-ready data layer in a medallion architecture |
| IsolationForest | An unsupervised anomaly detection algorithm that isolates outliers through random partitioning |
| Jitter | A randomised delay added to retry logic to prevent thundering herd on service recovery |
| Kappa | A stream processing architecture using a single pipeline for both live and historical data |
| LIA | Legitimate Interests Assessment — a GDPR document justifying processing under Article 6(1)(f) |
| mTLS | Mutual TLS — both client and server authenticate with certificates |
| PIPEDA | Personal Information Protection and Electronic Documents Act — Canada's federal privacy law |
| Pseudonymisation | Replacing direct identifiers with a reversible token; data remains personal under GDPR |
| SCCs | Standard Contractual Clauses — EU-approved contractual safeguards for international data transfers |
| Shadow mode | Running a candidate model in parallel with production, logging outputs without acting on them |
| Silver | The validated, cleaned data layer in a medallion architecture |
| Token bucket | A rate limiting algorithm where tokens accumulate at a fixed rate and are consumed per request |
| WAL | Write-Ahead Log — SQLite mode that improves durability and concurrent read performance |
