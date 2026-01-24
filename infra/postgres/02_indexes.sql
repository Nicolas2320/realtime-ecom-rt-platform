-- Helpful indexes for API queries
CREATE INDEX IF NOT EXISTS idx_kpi_metric_window_desc
  ON serving.kpi_minute (metric, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_kpi_window_start
  ON serving.kpi_minute (window_start);

CREATE INDEX IF NOT EXISTS idx_alerts_metric_window_desc
  ON serving.alerts (metric, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_severity_window_desc
  ON serving.alerts (severity, window_start DESC);

