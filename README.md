# Real-Time E-Commerce Streaming Platform (Local-First Lakehouse)

A portfolio-grade **streaming analytics** platform you can run locally with one command:
**Redpanda (Kafka) -> Spark Structured Streaming (Bronze/Silver/Gold) -> MinIO (S3) + Postgres -> FastAPI + Streamlit**, plus an **EWMA + Z-score anomaly detector** writing alerts back to Postgres.

**Bronze/Silver/Gold lakehouse** (parquet on MinIO)  
**Silver data quality validations + quarantine**  
**Event-time KPIs** (1-minute windows) served via API  
**Anomaly detection** on KPIs (EWMA baseline + residual Z-score)

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
