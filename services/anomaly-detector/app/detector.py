import os
import time
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import psycopg


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/analytics")

METRICS = [m.strip() for m in os.getenv(
    "METRICS",
    "events_total,events_purchase,revenue,purchase_conversion,add_to_cart_rate"
).split(",") if m.strip()]

LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "180")) # How much KPI history is pulled from Postgres each poll
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "60")) # Minutes of residual history to compute mean/std
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.2")) # Smoothing factor for EWMA baseline, high alpha: less smoothing, low alpha: high smoothing

# Thresholds
WARN_Z = float(os.getenv("WARN_Z", "3.0"))
CRIT_Z = float(os.getenv("CRIT_Z", "5.0"))

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30")) # Poll every 30 secs
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "10")) # Do not spam repeated warnings for same metric
IGNORE_RECENT_MINUTES = int(os.getenv("IGNORE_RECENT_MINUTES", "1"))

# rate guardrail: only score rate metrics when denominator is large enough
MIN_DENOM_CLICKS = int(os.getenv("MIN_DENOM_CLICKS", "50"))
MIN_DENOM_ADD_TO_CART = int(os.getenv("MIN_DENOM_ADD_TO_CART", "30"))

RULE_NAME = os.getenv("RULE_NAME", "ewma_zscore_v1")


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def load_kpis(conn, metrics, start_dt, end_dt) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT window_start, metric, value
            FROM serving.kpi_minute
            WHERE metric = ANY(%s)
              AND window_start >= %s AND window_start <= %s
            ORDER BY window_start ASC;
            """,
            (metrics, start_dt, end_dt),
        )
        rows = cur.fetchall()

    if not rows:
        print(f"[DETECTOR] Empty dataframe")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["window_start", "metric", "value"])
    df["window_start"] = pd.to_datetime(df["window_start"])
    return df

# Transform long format into wide format
def pivot_kpis(df: pd.DataFrame) -> pd.DataFrame:
    # wide: index=window_start, columns=metric
    wide = df.pivot_table(index="window_start", columns="metric", values="value", aggfunc="last")
    wide = wide.sort_index()
    return wide


def ewma_baseline(x: pd.Series, alpha: float) -> pd.Series:
    return x.ewm(alpha=alpha, adjust=False).mean()

# Returns (z, mu, sigma (std)) for latest point, using previous w points (exclude current)
def zscore_latest(residuals: pd.Series, w: int) -> tuple[float | None, float | None, float | None]:
    if residuals.shape[0] < w + 2:
        return None, None, None

    hist = residuals.iloc[-(w+1):-1]
    mu = float(hist.mean())
    sigma = float(hist.std(ddof=0))
    if sigma == 0.0 or np.isnan(sigma):
        return None, mu, sigma

    current = float(residuals.iloc[-1])
    z = (current - mu) / sigma
    return float(z), mu, sigma


def should_score_rate(metric: str, row_latest: pd.Series) -> bool:
    # row_latest includes other KPI columns for the same window_start
    if metric == "purchase_conversion":
        clicks = row_latest.get("events_click", np.nan)
        return pd.notna(clicks) and clicks >= MIN_DENOM_CLICKS
    if metric == "add_to_cart_rate":
        clicks = row_latest.get("events_click", np.nan)
        return pd.notna(clicks) and clicks >= MIN_DENOM_CLICKS
    if metric == "checkout_rate":
        carts = row_latest.get("events_add_to_cart", np.nan)
        return pd.notna(carts) and carts >= MIN_DENOM_ADD_TO_CART
    return True


def make_alert_id(metric: str, window_start: pd.Timestamp) -> str:
    # deterministic for idempotency
    ts = window_start.to_pydatetime().isoformat()
    return f"{RULE_NAME}:{metric}:{ts}"


def insert_alert(conn, alert: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO serving.alerts
              (alert_id, window_start, metric, value, baseline, score, severity, rule_name, details)
            VALUES
              (%(alert_id)s, %(window_start)s, %(metric)s, %(value)s, %(baseline)s, %(score)s,
               %(severity)s, %(rule_name)s, %(details)s::jsonb)
            ON CONFLICT (alert_id) DO NOTHING;
            """,
            alert,
        )
    conn.commit()


def load_last_alerts(conn) -> dict[str, datetime]:
    # last alert window_start per metric for this rule_name (cooldown across restarts)
    last = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metric, MAX(window_start)
            FROM serving.alerts
            WHERE rule_name = %s
            GROUP BY metric;
            """,
            (RULE_NAME,),
        )
        for metric, ws in cur.fetchall():
            if ws is not None:
                last[str(metric)] = ws
    return last


def main():
    print(f"[detector] rule={RULE_NAME} metrics={METRICS} poll={POLL_SECONDS}s")
    last_alert_ws = {}
    last_seen_max_ts = None

    while True:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                if not last_alert_ws:
                    last_alert_ws = load_last_alerts(conn)

                now = utc_now_naive()
                end_dt = now - timedelta(minutes=IGNORE_RECENT_MINUTES)
                start_dt = end_dt - timedelta(minutes=LOOKBACK_MINUTES)

                df = load_kpis(conn, METRICS + ["events_click","events_add_to_cart","events_checkout"], start_dt, end_dt)
                if df.empty:
                    print("[detector] no KPI data yet")
                    time.sleep(POLL_SECONDS)
                    continue

                wide = pivot_kpis(df)

                # need the most recent timestamp that has data
                if wide.shape[0] < ROLLING_WINDOW + 2:
                    print(f"[detector] not enough history yet: rows={wide.shape[0]}")
                    time.sleep(POLL_SECONDS)
                    continue

                idx = wide.index.sort_values()

                if len(idx) < 2:
                    time.sleep(POLL_SECONDS)
                    continue

                current_max = idx[-1]
                previous_ts = idx[-2]

                if last_seen_max_ts == current_max:
                    time.sleep(POLL_SECONDS)
                    continue

                latest_ts = previous_ts
                last_seen_max_ts = current_max

                latest_row = wide.loc[latest_ts]

                for metric in METRICS:
                    if metric not in wide.columns:
                        continue

                    series = wide[metric].dropna()
                    series_cut = series.loc[:latest_ts]
                    if series_cut.shape[0] < ROLLING_WINDOW + 2:
                        continue

                    # guardrail for rates
                    if not should_score_rate(metric, latest_row):
                        continue

                    
                    baseline = ewma_baseline(series_cut, EWMA_ALPHA)
                    residuals = series_cut - baseline

                    z, mu, sigma = zscore_latest(residuals, ROLLING_WINDOW)
                    if z is None:
                        continue

                    absz = abs(z)
                    if absz < WARN_Z:
                        continue

                    severity = "critical" if absz >= CRIT_Z else "warning"

                    # cooldown (unless critical)
                    prev = last_alert_ws.get(metric)
                    if prev is not None and severity != "critical":
                        if latest_ts.to_pydatetime() - prev < timedelta(minutes=COOLDOWN_MINUTES):
                            continue

                    value = float(series.loc[latest_ts])
                    base = float(baseline.loc[latest_ts])

                    alert = {
                        "alert_id": make_alert_id(metric, latest_ts),
                        "window_start": latest_ts.to_pydatetime(),
                        "metric": metric,
                        "value": value,
                        "baseline": base,
                        "score": float(z),
                        "severity": severity,
                        "rule_name": RULE_NAME,
                        "details": json.dumps({
                            "method": "ewma+zscore(residual)",
                            "ewma_alpha": EWMA_ALPHA,
                            "rolling_window": ROLLING_WINDOW,
                            "warn_z": WARN_Z,
                            "crit_z": CRIT_Z,
                            "mu": mu,
                            "sigma": sigma,
                            "ignore_recent_minutes": IGNORE_RECENT_MINUTES,
                            "cooldown_minutes": COOLDOWN_MINUTES,
                        }),
                    }
                    insert_alert(conn, alert)
                    last_alert_ws[metric] = latest_ts.to_pydatetime()
                    print(f"[ALERT] {severity} metric={metric} ts={latest_ts} value={value:.4f} baseline={base:.4f} z={z:.2f}")

        except Exception as e:
            print(f"[detector][error] {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
