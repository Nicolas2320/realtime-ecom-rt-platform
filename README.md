# Real-Time E-Commerce Streaming Platform (Local-First Lakehouse)

This project simulates a real production scenario: **high-volume e-commerce events** (click, add_to_cart, checkout, purchase) arriving in real time, validated and organized into a **lakehouse** (Bronze/Silver/Gold), aggregated into **event-time KPIs**, and served through an API + dashboard, while an **anomaly detector** continuously monitors metrics and writes alerts back to the serving layer.

It’s designed to demonstrate end-to-end **data engineering competencies**:
- streaming ingestion (Kafka/Redpanda) and exactly-once patterns (checkpoints + idempotent writes)
- data quality enforcement with **quarantine** (no data loss, observable failures)
- lakehouse modeling (Bronze/Silver/Gold on S3/MinIO with partitions)
- KPI serving with Postgres + FastAPI
- monitoring/alerting with EWMA + Z-score anomaly detection

---

## One-command run

Start infrastructure + UI + pipeline services:

```bash
make run
```

Start traffic (event generator):

```bash
make events
```

### Links (local)

- Redpanda Console: http://localhost:8080
- MinIO Console: http://localhost:9001
 (user: minio, pass: minio12345)
- FastAPI OpenAPI docs: http://localhost:8000/docs
- Streamlit dashboard: http://localhost:8501

More details:
- `docs/runbook.md`

## Data layers (Bronze / Silver / Gold)

### 1) Bronze (raw-ish, append-only)

**Input:** Kafka topic `ecom.events.raw.v1`

**Output:**  `s3a://lake/bronze/ecom_events/v1/`
Includes:

- `raw_json`
- Kafka metadata (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_key`, `kafka_timestamp`)
- Parsed columns (best-effort typed)
- Partitions by **ingest_date** / **ingest_hour**

### 2) Silver (validated + deduped)

**Input:** Bronze parquet

**Outputs:**

- Silver: `s3a://lake/silver/ecom_events/v1/` (partition **event_date/event_hour**)
- Quarantine: `s3a://lake/quarantine/ecom_events/v1/` (partition **ingest_date/ingest_hour**)

Silver enforces:

- required fields + timestamp parse

- allowed `event_type`

- event-type-specific rules (see below)

- dedupe by `event_id` with watermark

More details:

- `docs/dq_rules.md`
- `docs/data_contract.md`

### 3) Gold (KPIs in Postgres)

**Input:** Silver parquet

**Output:** Postgres `serving.kpi_minute` (long format)

KPIs per 1-minute event-time window:

- `events_total`, `events_click`, `events_add_to_cart`, `events_checkout`, `events_purchase`

- `revenue`, `aov`

- `add_to_cart_rate`, `checkout_rate`, `purchase_conversion`

Gold uses **foreachBatch** and rewrites rows per window for idempotency.

### 4) Event contract (what the generator produces)

Full examples + invalid examples: `docs/data_contract.md`

## Handy Make targets

```bash
make ps               # container status
make logs             # follow all logs
make topics           # create Kafka topic
make events           # start generator (traffic)
make bronze-tree      # list Bronze files in MinIO
make silver-tree      # list Silver files in MinIO
make quarantine-tree  # list Quarantine files in MinIO
make kpi-sample       # sample KPIs from Postgres
make alerts-sample    # sample alerts from Postgres
make psql             # interactive psql
make clean            # nuke volumes (full reset)
```

## Repo Structure

```bash
docs/                       # architecture, runbook, contracts, dq rules
infra/postgres/             # schema + indexes
services/
  generator/                # produces Kafka events
  bronze-writer/            # Kafka -> Bronze parquet
  silver-writer/            # Bronze -> Silver + Quarantine
  gold-writer/              # Silver -> Postgres KPIs
  anomaly-detector/         # KPI polling + alerts
  api/                      # FastAPI serving KPIs/alerts
  dashboard/                # Streamlit UI
docker-compose.yml
Makefile
```
