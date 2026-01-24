from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
import os
import psycopg

app = FastAPI(title="Real-Time E-commerce Platform API", version="0.1.0")

DATABASE_URL = os.getenv("DATABASE_URL", "")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/deps")
def deps():
    if not DATABASE_URL:
        return {"postgres": {"ok": False, "error": "DATABASE_URL not set"}}

    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return {"postgres": {"ok": True}}
    except Exception as e:
        return {"postgres": {"ok": False, "error": str(e)}}
    
def parse_dt(s: str) -> datetime:
    # accepts ISO like 2026-01-08T10:00:00Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

@app.get("/kpis")
def kpis(
    metric: str,
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
    limit: int = 500
):
    if not DATABASE_URL:
        raise HTTPException(500, "DATABASE_URL not set")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    to_dt = parse_dt(to_ts) if to_ts else now
    from_dt = parse_dt(from_ts) if from_ts else (to_dt - timedelta(minutes=60))

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT window_start, value
                FROM serving.kpi_minute
                WHERE metric = %s AND window_start >= %s AND window_start <= %s
                ORDER BY window_start ASC
                LIMIT %s;
                """,
                (metric, from_dt, to_dt, limit),
            )
            rows = cur.fetchall()

    return {
        "metric": metric,
        "from": from_dt.isoformat() + "Z",
        "to": to_dt.isoformat() + "Z",
        "points": [{"ts": r[0].isoformat() + "Z", "value": float(r[1])} for r in rows],
    }

@app.get("/kpis/latest")
def kpis_latest(metrics: str = "revenue,events_purchase,purchase_conversion,events_total"):
    if not DATABASE_URL:
        raise HTTPException(500, "DATABASE_URL not set")

    metric_list = [m.strip() for m in metrics.split(",") if m.strip()]
    if not metric_list:
        raise HTTPException(400, "metrics is empty")

    # latest per metric
    result = {}
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for m in metric_list:
                cur.execute(
                    """
                    SELECT window_start, value
                    FROM serving.kpi_minute
                    WHERE metric = %s
                    ORDER BY window_start DESC
                    LIMIT 1;
                    """,
                    (m,),
                )
                row = cur.fetchone()
                result[m] = None if row is None else {"ts": row[0].isoformat() + "Z", "value": float(row[1])}

    return {"latest": result}

@app.get("/alerts")
def alerts(
    metric: Optional[str] = None,
    severity: Optional[str] = None,
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
    limit: int = 200
):
    if not DATABASE_URL:
        raise HTTPException(500, "DATABASE_URL not set")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    to_dt = parse_dt(to_ts) if to_ts else now
    from_dt = parse_dt(from_ts) if from_ts else (to_dt - timedelta(minutes=180))

    where = ["window_start >= %s", "window_start <= %s"]
    params = [from_dt, to_dt]

    if metric:
        where.append("metric = %s")
        params.append(metric)
    if severity:
        where.append("severity = %s")
        params.append(severity)

    sql = f"""
      SELECT alert_id, created_at, window_start, metric, value, baseline, score, severity, rule_name, details
      FROM serving.alerts
      WHERE {' AND '.join(where)}
      ORDER BY window_start DESC
      LIMIT %s;
    """
    params.append(limit)

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    def row_to_obj(r):
        return {
            "alert_id": r[0],
            "created_at": r[1].isoformat() + "Z",
            "window_start": r[2].isoformat() + "Z",
            "metric": r[3],
            "value": float(r[4]),
            "baseline": None if r[5] is None else float(r[5]),
            "score": None if r[6] is None else float(r[6]),
            "severity": r[7],
            "rule_name": r[8],
            "details": r[9],
        }

    return {"from": from_dt.isoformat() + "Z", "to": to_dt.isoformat() + "Z", "alerts": [row_to_obj(r) for r in rows]}


@app.get("/alerts/latest")
def alerts_latest(limit: int = 50):
    if not DATABASE_URL:
        raise HTTPException(500, "DATABASE_URL not set")

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT alert_id, created_at, window_start, metric, value, baseline, score, severity, rule_name, details
                FROM serving.alerts
                ORDER BY window_start DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()

    return {
        "alerts": [
            {
                "alert_id": r[0],
                "created_at": r[1].isoformat() + "Z",
                "window_start": r[2].isoformat() + "Z",
                "metric": r[3],
                "value": float(r[4]),
                "baseline": None if r[5] is None else float(r[5]),
                "score": None if r[6] is None else float(r[6]),
                "severity": r[7],
                "rule_name": r[8],
                "details": r[9],
            }
            for r in rows
        ]
    }
