import os
import requests
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Real-Time E-com Analytics", layout="wide")

API_URL = os.getenv("API_URL", "http://api:8000")

# --- controls ---
st.sidebar.header("Controls")
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
refresh_secs = st.sidebar.slider("Refresh interval (sec)", 5, 60, 15, 5)
minutes = st.sidebar.slider("Lookback window (minutes)", 15, 360, 60, 15)

metric = st.sidebar.selectbox(
    "Metric",
    ["revenue", "events_total", "events_purchase", "purchase_conversion", "add_to_cart_rate"],
    index=0
)

severity_filter = st.sidebar.selectbox("Alert severity", ["all", "warning", "critical"], index=0)

if auto_refresh:
    st_autorefresh(interval=refresh_secs * 1000, key="refresh")

st.title("Real-Time E-commerce Analytics Platform")
st.caption("Live dashboard + anomaly detection + alerts")

@st.cache_data(ttl=10)
def get_latest():
    r = requests.get(f"{API_URL}/kpis/latest", timeout=3)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=10)
def get_series(metric: str, minutes: int):
    r = requests.get(f"{API_URL}/kpis", params={"metric": metric}, timeout=3)
    r.raise_for_status()
    # API defaults to last 60 min; we’ll trim client-side if needed
    data = r.json()["points"]
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    df["value"] = df["value"].astype(float)
    # trim to requested minutes
    cutoff = df["ts"].max() - pd.Timedelta(minutes=minutes)
    df = df[df["ts"] >= cutoff]
    return df.sort_values("ts")

@st.cache_data(ttl=10)
def get_alerts(minutes: int, severity: str):
    params = {"limit": 200}
    if severity != "all":
        params["severity"] = severity
    r = requests.get(f"{API_URL}/alerts", params=params, timeout=3)
    r.raise_for_status()
    alerts = r.json()["alerts"]
    df = pd.DataFrame(alerts)
    if df.empty:
        return df
    df["window_start"] = pd.to_datetime(df["window_start"])
    cutoff = df["window_start"].max() - pd.Timedelta(minutes=minutes)
    df = df[df["window_start"] >= cutoff]
    return df.sort_values("window_start", ascending=False)

# --- KPI tiles ---
colA, colB, colC, colD = st.columns(4)
try:
    latest = get_latest()["latest"]
    colA.metric("Revenue (latest)", "-" if latest.get("revenue") is None else f"{latest['revenue']['value']:.2f}")
    colB.metric("Purchases (latest)", "-" if latest.get("events_purchase") is None else f"{latest['events_purchase']['value']:.0f}")
    colC.metric("Total events (latest)", "-" if latest.get("events_total") is None else f"{latest['events_total']['value']:.0f}")
    colD.metric("Conversion (latest)", "-" if latest.get("purchase_conversion") is None else f"{latest['purchase_conversion']['value']:.4f}")
except Exception as e:
    st.warning(f"Latest KPIs not available yet: {e}")

st.divider()

# --- KPI chart ---
st.subheader(f"KPI time series: {metric}")
try:
    sdf = get_series(metric, minutes)
    if sdf.empty:
        st.info("No KPI points yet. Make sure gold-writer is running.")
    else:
        chart_df = sdf.set_index("ts")[["value"]]
        st.line_chart(chart_df)
except Exception as e:
    st.error(f"Failed to load KPI series: {e}")

st.divider()

# --- Alerts table ---
st.subheader("Alerts")
try:
    adf = get_alerts(minutes, severity_filter)
    if adf.empty:
        st.info("No alerts yet (or detector not running).")
    else:
        # keep the table readable
        show_cols = ["window_start", "metric", "severity", "value", "baseline", "score", "rule_name"]
        st.dataframe(adf[show_cols], use_container_width=True)
except Exception as e:
    st.error(f"Failed to load alerts: {e}")

st.caption("Tip: to trigger alerts quickly, temporarily stop the generator (drop) or increase EPS (spike).")
