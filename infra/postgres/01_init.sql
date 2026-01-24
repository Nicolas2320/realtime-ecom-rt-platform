CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.kpi_minute (
  window_start TIMESTAMP NOT NULL,
  metric       TEXT NOT NULL,
  value        DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (window_start, metric)
);

CREATE TABLE IF NOT EXISTS serving.alerts (
  alert_id      TEXT PRIMARY KEY,
  created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
  window_start  TIMESTAMP NOT NULL,
  metric        TEXT NOT NULL,
  value         DOUBLE PRECISION NOT NULL,
  baseline      DOUBLE PRECISION,
  score         DOUBLE PRECISION,
  severity      TEXT NOT NULL,
  rule_name     TEXT NOT NULL,
  details       JSONB
);
