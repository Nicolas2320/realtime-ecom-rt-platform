# Runbook

This repo runs a local, end-to-end streaming pipeline:

**Generator -> Redpanda -> Spark (Bronze/Silver/Gold) -> MinIO + Postgres -> FastAPI -> Streamlit**, plus a polling **Anomaly Detector** that writes alerts back to Postgres.

---

## Pre-requisites

- Docker Desktop / Docker Engine + Docker Compose v2
- ~8GB RAM available for Docker (Spark + Postgres + Redpanda)

Ports used:
- Redpanda Console: `http://localhost:8080`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- FastAPI: `http://localhost:8000`
- Streamlit: `http://localhost:8501`
- Postgres: `localhost:5432` (user/pass/db: `app/app/analytics`)

---

## Quick start

1) Start core services (MinIO, FastAPI, Streamlit, Postgres, Redpanda):

```bash
make up
```

2) Create Kafka topic:

```bash
make topics
```

3) Start the events generator:

```bash
make events
```

4) Start the all the pipeline (Bronze/Silver/Gold) + anomaly detector:

```bash
make run
```

Useful status checks:

```bash
make ps
make logs
```

---

## What “good” looks like

### 1) Redpanda
- Console shows topic `ecom.events.raw.v1` receiving messages.

### 2) MinIO (lake bucket)
- Bronze partitions appear:

```bash
make bronze-tree
```

- Silver & Quarantine:

```bash
make silver-tree
make quarantine-tree
```

### 3) Postgres
- KPIs start populating:

```bash
make kpi-sample
```

- Alerts start populating (after enough KPI history data):

```bash
make alerts-sample
```

### 4) API
- Health:

```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8000/deps
```

- Latest KPIs / alerts:

```bash
curl -s http://localhost:8000/kpis/latest
curl -s http://localhost:8000/alerts/latest
```

---

## Debugging checklist

### Service logs

```bash
docker compose logs -f redpanda-0
docker compose logs -f bronze-writer
docker compose logs -f silver-writer
docker compose logs -f gold-writer
docker compose logs -f anomaly-detector
```

### MinIO file listing

```bash
make bronze-tree
make silver-tree
make quarantine-tree
```

### Postgres queries

```bash
make psql
-- inside psql:
\dt serving.*
SELECT * FROM serving.kpi_minute ORDER BY window_start DESC, metric LIMIT 20;
SELECT * FROM serving.alerts ORDER BY window_start DESC LIMIT 20;
```

---

## Common failures (and fixes)

### 1) Topic doesn’t exist / generator not producing
**Symptoms**: Bronze writer has no input; Redpanda console shows no traffic.

**Fix**:
```bash
make topics
make events
```

### 2) Bronze/Silver/Gold “waiting for path”
`silver-writer` waits for `BRONZE_PATH` and `gold-writer` waits for `SILVER_PATH`.

**Fix**:
- Ensure upstream service is running (`make ps`).
- Confirm data exists in MinIO (`make bronze-tree` / `make silver-tree`).

### 3) Spark dependency download issues (Ivy / coursier)
**Symptoms**: Spark fails early with download/permission errors.

**Fix**:
- Re-run with a clean ivy volume:
  ```bash
  make clean
  docker volume rm realtime-ecom-platform_spark_ivy || true
  make up
  ```

### 4) Postgres not ready / API errors
**Symptoms**: API returns 500 or `/deps` shows Postgres error.

**Fix**:
```bash
docker compose logs -f postgres
# then, to confirm
curl -s http://localhost:8000/deps
```

### 5) No alerts
Alerts require enough KPI history data and the detector ignores the recent minute(s) (`IGNORE_RECENT_MINUTES`).

**Fix**:
- Let the pipeline run a bit longer.
- Lower `ROLLING_WINDOW` and thresholds for a quicker demo (see `docker-compose.yml` env for `anomaly-detector`).

---

## Reset everything

- Stop and remove containers:
```bash
make down-all
```

- Full reset (including volumes / data):
```bash
make clean
```
