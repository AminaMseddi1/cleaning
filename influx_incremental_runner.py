#!/usr/bin/env python3
import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler

import pandas as pd
import requests

# =====================
# InfluxDB connection
# =====================
INFLUX_URL = "http://192.168.70.33:30344/api/v2/query?orgID=09be8a13d1a6075a"
INFLUX_TOKEN = "w08AWD3J22XF0rllTvRX3xhXRnUKLycim8LSMpgIHme5PaMreu19u6neyb45tYOmlM4WUQEVh6ckIoL18c2TUw=="
BUCKET = "edgex"

OUTPUT_DIR = "/opt/influx_cleaner"
RAW_CSV = os.path.join(OUTPUT_DIR, "historical_data_raw.csv")
STATE_FILE = os.path.join(OUTPUT_DIR, "state.json")

# Target resources to split into separate CSVs
RESOURCES = ["Ph value", "TDS value", "Temperature", "distance", "intensity", "humidity"]

# =====================
# Time window controls
# =====================
# Fixed start of the analysis window (UTC)
START_UTC = datetime(2024, 12, 11, 0, 0, 0, tzinfo=timezone.utc)
# Upper bound (inclusive) on 2025-02-14
FINAL_UTC = datetime(2025, 2, 14, 23, 59, 59, tzinfo=timezone.utc)

# Each cycle runs every 4 hours and extends stop time by +4h cumulatively
STEP = timedelta(hours=4)

# =====================
# HTTP Server for CSVs
# =====================
def serve_csvs():
    """Serve the OUTPUT_DIR over HTTP on port 8080."""
    os.chdir(OUTPUT_DIR)
    server = HTTPServer(("0.0.0.0", 8080), SimpleHTTPRequestHandler)
    print("🔗 Serving CSVs on http://0.0.0.0:8080")
    server.serve_forever()

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_state():
    """Load or initialize state with current_stop = START + 4h."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # parse ISO 8601 stored timestamp
                current_stop = datetime.fromisoformat(data["current_stop"])
                return current_stop
        except Exception as e:
            print(f"⚠️  Could not read state file, reinitializing. Reason: {e}")
    # default initial stop is START + 4h
    return START_UTC + STEP

def save_state(current_stop: datetime):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"current_stop": current_stop.isoformat()}, f)

def iso_z(dt: datetime) -> str:
    """Format datetime in ISO 8601 with Z suffix (UTC)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def build_flux_query(start_dt: datetime, stop_dt: datetime) -> str:
    """Build the Flux query string for the given window [start_dt, stop_dt]."""
    return f"""
from(bucket: "{BUCKET}")
  |> range(start: {iso_z(start_dt)}, stop: {iso_z(stop_dt)})
  |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
  |> filter(fn: (r) => r["_field"] == "origin")
"""

def fetch_influx_csv(flux_query: str) -> bytes:
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Accept": "application/csv",
        "Content-type": "application/vnd.flux"
    }
    resp = requests.post(INFLUX_URL, headers=headers, data=flux_query, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"InfluxDB query failed: HTTP {resp.status_code} - {resp.text[:500]}")
    return resp.content

def process_csv_and_save_splits():
    # Load CSV and clean
    df = pd.read_csv(RAW_CSV, low_memory=False)

    # Keep only relevant columns
    columns_to_keep = ["_time", "resourceName", "value", "deviceName"]
    df = df[[c for c in columns_to_keep if c in df.columns]]

    # Parse _time and drop invalid rows
    df["_time"] = pd.to_datetime(df["_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["_time"])

    # Ensure 'value' is numeric only, drop non-numeric entries
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    # Resample every 4 hours and compute mean of 'value'
    df_4h = (
        df.set_index("_time")
          .groupby("resourceName")
          .resample("4h")["value"]
          .mean()
          .reset_index()
    )

    # Format 'value' to remove scientific notation
    df_4h["value"] = df_4h["value"].map(lambda x: f"{x:.6f}")

    # Format _time for Grafana
    df_4h["_time"] = pd.to_datetime(df_4h["_time"], utc=True)
    df_4h["_time"] = df_4h["_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Split by resourceName and save CSVs
    for res in RESOURCES:
        df_res = df_4h[df_4h["resourceName"] == res]
        out_file = os.path.join(OUTPUT_DIR, f"cleaned_{res.replace(' ', '_').lower()}.csv")
        if not df_res.empty:
            df_res.to_csv(out_file, index=False)
            print(f"💾 Saved {res} → {out_file}")
        else:
            print(f"ℹ️  No data found for {res} (skipped)")

    # Summary by resource (original df, keeps deviceName)
    res_summary = (
        df.groupby("resourceName")
          .agg(rows=("value", "size"),
               devices=("deviceName", "nunique"),
               first_ts=("_time", "min"),
               last_ts=("_time", "max"))
          .reset_index()
          .sort_values("rows", ascending=False)
    )
    print("✅ Summary by resource:")
    try:
        # Pretty print without truncation
        with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", 120):
            print(res_summary)
    except Exception:
        print(res_summary)

    # Overall time coverage
    time_min = df["_time"].min()
    time_max = df["_time"].max()
    print(f"🕒 Overall time coverage in this run: {time_min} → {time_max}")

def run_once(current_stop: datetime) -> datetime:
    """Run one cycle: query from START_UTC to current_stop, process and save outputs.
       Returns the next stop (current_stop + STEP), capped at FINAL_UTC.
    """
    stop_capped = min(current_stop, FINAL_UTC)
    if stop_capped < START_UTC + STEP:
        stop_capped = START_UTC + STEP

    flux_query = build_flux_query(START_UTC, stop_capped)
    print(f"▶️  Querying InfluxDB range: {iso_z(START_UTC)} → {iso_z(stop_capped)}")

    content = fetch_influx_csv(flux_query)
    with open(RAW_CSV, "wb") as f:
        f.write(content)
    print(f"📥 Raw data saved to {RAW_CSV} ({len(content)} bytes)")

    # Process and write splits
    process_csv_and_save_splits()

    # Calculate next stop
    next_stop = stop_capped + STEP
    save_state(next_stop)
    print(f"✅ Cycle complete. Next stop will be: {iso_z(next_stop)} (capped at {iso_z(FINAL_UTC)})")

    return next_stop

def main():
    ensure_dirs()

    # Start HTTP server thread once
    threading.Thread(target=serve_csvs, daemon=True).start()

    current_stop = load_state()
    print(f"🔁 Starting scheduler. Initial current_stop: {iso_z(current_stop)}")
    print(f"📅 Fixed start: {iso_z(START_UTC)} | Final cap: {iso_z(FINAL_UTC)}")

    while True:
        try:
            # If we've already passed the final window, exit gracefully
            if current_stop > FINAL_UTC:
                print("🏁 Reached final window (≥ 2025-02-14). Exiting loop.")
                break

            current_stop = run_once(current_stop)

        except Exception as e:
            print(f"❌ Error during cycle: {e}")

        # Sleep exactly 4 hours between cycles
        print("⏱️  Sleeping for 4 hours...")
        time.sleep(int(STEP.total_seconds()))

if __name__ == "__main__":
    main()
