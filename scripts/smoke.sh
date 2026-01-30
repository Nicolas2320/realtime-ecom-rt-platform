#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"

MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-600}"  # 10 min local
SLEEP_SECONDS=5
MAX_ITERS=$((MAX_WAIT_SECONDS / SLEEP_SECONDS))

cleanup() {
  echo ""
  echo "[SMOKE] cleanup: docker compose down -v --remove-orphans"
  make clean
  make down-all
}
trap cleanup EXIT

echo "[SMOKE] 1) starting base services (postgres/minio/redpanda/api/dashboard...)"
  docker compose up -d --build

echo "[SMOKE] 2) creating topic"
  docker exec -i redpanda-0 rpk topic create ecom.events.raw.v1 --brokers localhost:9092 -p 3 -r 1 || true

echo "[SMOKE] 3) starting pipeline (bronze/silver/gold)"
	docker compose --profile pipeline up -d --build bronze-writer
	docker compose --profile pipeline up -d --build silver-writer
	docker compose --profile pipeline up -d --build gold-writer

echo "[SMOKE] 4) starting generator (traffic)"
	docker compose --profile events up -d --build generator

echo "[SMOKE] 4) Waiting 60 seconds for generator to produce data..."
  sleep 60

echo "[SMOKE] 5) starting anomaly detector"
	docker compose --profile detector up -d --build anomaly-detector

echo "[SMOKE] 6) waiting for API /health..."
API_OK=0
for i in {1..60}; do
  if curl -fsS "$API_URL/health"; then
    API_OK=1
    echo "[SMOKE] 6) API is up"
    break
  fi
  sleep 1
done

if [[ $API_OK -ne 1 ]]; then
  echo "[SMOKE] 6) ERROR: API never became healthy"
  exit 1
fi

echo "[SMOKE] 7) deps check (/deps)..."
  DEPS_OK=0
  for i in {1..60}; do
    if curl -fsS "$API_URL/deps"; then
      DEPS_OK=1
      echo "[SMOKE] 7) connection API-postgres is up"
      break
    fi
    sleep 1
  done

  if [[ $DEPS_OK -ne 1 ]]; then
    echo "[SMOKE] 7) ERROR: /deps never became ready"
    exit 1
  fi

echo "[SMOKE] 8) waiting for KPI rows in Postgres (serving.kpi_minute)..."
  KPI_OK=0
  for ((i=1; i<=MAX_ITERS; i++)); do
    COUNT=$(docker exec -i postgres psql -U app -d analytics -tA -v ON_ERROR_STOP=1 -c \
      "SELECT COUNT(*) FROM serving.kpi_minute;" || echo "0")

    COUNT="$(echo "$COUNT" | tr -d '[:space:]')"

    if [[ "${COUNT:-0}" =~ ^[0-9]+$ ]] && (( COUNT > 0 )); then
      KPI_OK=1
      echo "[SMOKE] 8) KPI rows found (count=$COUNT)"
      break
    fi
    sleep $SLEEP_SECONDS
  done

  if [[ $KPI_OK -ne 1 ]]; then
    echo "[SMOKE] 8) ERROR: No KPI rows appeared in last minutes"
    echo "[SMOKE] 8) TIP: check logs: docker compose logs -f gold-writer"

    exit 1
  fi

echo "[SMOKE] 9) checking /kpi/latest returns OK..."
curl -fsS "$API_URL/kpis/latest"
echo "[SMOKE] 9) /alerts/latest OK"

echo "[SMOKE] 10) checking /alerts/latest returns OK..."
curl -fsS "$API_URL/alerts/latest"
echo "[SMOKE] 10) /alerts/latest OK"

  echo ""
  echo "[SMOKE][DEBUG] Minio lake tree (bronze/silver):"
  docker compose --profile debug run --rm mc tree local/lake/bronze/ || true
  docker compose --profile debug run --rm mc tree local/lake/silver/ || true

echo "[SMOKE] SUCCESS (end-to-end system works)"
