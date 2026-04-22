"""
AARAMBH_SYSTEM — Live Space Data Collector
==========================================
Polls CelesTrak (GP elements) and NOAA (space weather) APIs every 10 minutes,
saves timestamped JSON snapshots, and provides merge + verification utilities.

Author : AARAMBH Team
Tested : Windows 11 / Python 3.10+
"""

import os
import sys
import json
import time
import glob
import logging
import requests
import schedule
import pandas as pd
from datetime import datetime, timezone
from tqdm import tqdm

# ────────────────────────────────────────────────────────────
#  CONFIGURATION
# ────────────────────────────────────────────────────────────
# Resolve paths relative to the project root (parent of data_ingestion/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# CelesTrak GP elements (last-30-days catalog, JSON format)
# GROUP=last-30-days gives recently updated objects; use GROUP=active for the full active catalog
CELESTRAK_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=last-30-days&FORMAT=json"

# NOAA SWPC space-weather endpoints
NOAA_SOURCES = {
    "planetary_k_index_1m": "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",
    "solar_wind_plasma_1day": "https://services.swpc.noaa.gov/products/solar-wind/plasma-1-day.json",
    "solar_wind_mag_1day": "https://services.swpc.noaa.gov/products/solar-wind/mag-1-day.json",
    "enlil_time_series": "https://services.swpc.noaa.gov/json/enlil_time_series.json",
}

# HTTP settings
REQUEST_TIMEOUT = 60          # seconds per request
RETRY_DELAY_BASE = 10         # base back-off seconds on error
MAX_RETRIES = 3               # retries per source before skipping
HTTP_HEADERS = {
    "User-Agent": "AARAMBH_SYSTEM/1.0 (Space ML Research; Python/requests)",
    "Accept": "application/json",
}

# ────────────────────────────────────────────────────────────
#  LOGGING SETUP (console + file)
# ────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(PROJECT_ROOT, "data_ingestion", "collector.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("aarambh.collector")


# ────────────────────────────────────────────────────────────
#  HELPERS
# ────────────────────────────────────────────────────────────
def _ensure_dirs():
    """Create data/ directory if it doesn't exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(f"Data directory ready → {DATA_DIR}")


def _timestamp() -> str:
    """Return a filesystem-safe UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_request(url: str, label: str) -> requests.Response | None:
    """
    GET *url* with retries and exponential back-off.
    Returns Response on success, None on exhausted retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HTTP_HEADERS)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                wait = RETRY_DELAY_BASE * attempt
                logger.warning(
                    f"[{label}] Rate-limited (429). Sleeping {wait}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
            else:
                logger.error(
                    f"[{label}] HTTP {resp.status_code} on attempt "
                    f"{attempt}/{MAX_RETRIES}"
                )
                time.sleep(RETRY_DELAY_BASE)
        except requests.exceptions.Timeout:
            logger.warning(
                f"[{label}] Timeout on attempt {attempt}/{MAX_RETRIES}"
            )
            time.sleep(RETRY_DELAY_BASE)
        except requests.exceptions.ConnectionError as exc:
            logger.warning(
                f"[{label}] Connection error: {exc} "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(RETRY_DELAY_BASE * attempt)
        except requests.exceptions.RequestException as exc:
            logger.error(f"[{label}] Request error: {exc}")
            time.sleep(RETRY_DELAY_BASE)
    logger.error(f"[{label}] All {MAX_RETRIES} attempts failed — skipping.")
    return None


def _save_json(data, filepath: str) -> bool:
    """Write *data* as pretty-printed JSON. Returns True on success."""
    try:
        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
        size_kb = os.path.getsize(filepath) / 1024
        logger.info(f"  ✓ Saved {filepath} ({size_kb:.1f} KB)")
        return True
    except (OSError, TypeError) as exc:
        logger.error(f"  ✗ Failed to write {filepath}: {exc}")
        return False


def _latest_file(pattern: str) -> str | None:
    """Return the most-recently modified file matching *pattern*, or None."""
    files = glob.glob(os.path.join(DATA_DIR, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


# ────────────────────────────────────────────────────────────
#  DATA COLLECTION
# ────────────────────────────────────────────────────────────
def fetch_celestrak(ts: str | None = None) -> str | None:
    """
    Download the full CelesTrak GP-elements catalog and save to disk.
    Returns the saved filepath on success, None on failure.
    """
    ts = ts or _timestamp()
    logger.info("Fetching CelesTrak GP catalog …")
    resp = _safe_request(CELESTRAK_URL, "CelesTrak")
    if resp is None:
        return None

    try:
        payload = resp.json()
    except (json.JSONDecodeError, ValueError):
        logger.error(f"CelesTrak returned non-JSON payload: {resp.text[:200]}")
        return None

    # Validate we got a list of satellite objects
    if not isinstance(payload, list) or len(payload) == 0:
        logger.error(f"CelesTrak returned unexpected data type: {type(payload)}")
        return None

    filepath = os.path.join(DATA_DIR, f"satellites_live_{ts}.json")
    if _save_json(payload, filepath):
        logger.info(f"  → {len(payload)} satellites ingested.")
        return filepath
    return None


def fetch_noaa(ts: str | None = None) -> dict[str, str | None]:
    """
    Download all four NOAA space-weather datasets.
    Returns dict mapping source name → saved filepath (or None on failure).
    """
    ts = ts or _timestamp()
    logger.info("Fetching NOAA space-weather data …")
    results: dict[str, str | None] = {}

    for name, url in tqdm(NOAA_SOURCES.items(), desc="NOAA sources", ncols=80):
        resp = _safe_request(url, f"NOAA/{name}")
        if resp is None:
            results[name] = None
            continue

        try:
            payload = resp.json()
        except json.JSONDecodeError:
            logger.error(f"[NOAA/{name}] Non-JSON response — skipped.")
            results[name] = None
            continue

        filepath = os.path.join(DATA_DIR, f"noaa_{name}_{ts}.json")
        results[name] = filepath if _save_json(payload, filepath) else None

    return results


def collect_all() -> None:
    """
    Single collection cycle: fetch CelesTrak + NOAA, log summary.
    Designed to be called by the scheduler.
    """
    _ensure_dirs()
    cycle_start = datetime.now(timezone.utc)
    ts = cycle_start.strftime("%Y%m%dT%H%M%SZ")

    logger.info("=" * 60)
    logger.info(f"COLLECTION CYCLE START — {cycle_start.isoformat()}")
    logger.info("=" * 60)

    sat_path = fetch_celestrak(ts)
    noaa_paths = fetch_noaa(ts)

    success_count = (1 if sat_path else 0) + sum(
        1 for v in noaa_paths.values() if v is not None
    )
    total = 1 + len(NOAA_SOURCES)
    elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()

    logger.info("-" * 60)
    logger.info(
        f"CYCLE DONE — {success_count}/{total} sources OK "
        f"({elapsed:.1f}s elapsed)"
    )
    logger.info("-" * 60)


# ────────────────────────────────────────────────────────────
#  MERGE / FEATURE ENGINEERING
# ────────────────────────────────────────────────────────────
def merge_live_data() -> pd.DataFrame:
    """
    Load the latest satellite JSON + latest NOAA files and merge into a
    single DataFrame with basic orbital-physics features.

    Returns
    -------
    pd.DataFrame
        One row per satellite with physics columns and the latest
        Kp-index / solar-wind plasma values broadcast across all rows.
    """
    logger.info("Merging latest live data …")

    # ── Satellites ──────────────────────────────────────────
    sat_file = _latest_file("satellites_live_*.json")
    if sat_file is None:
        logger.error("No satellite file found in data/.")
        return pd.DataFrame()

    with open(sat_file, "r", encoding="utf-8") as fh:
        sat_data = json.load(fh)
    df_sat = pd.DataFrame(sat_data)
    logger.info(f"  Loaded {len(df_sat)} satellites from {os.path.basename(sat_file)}")

    # ── Basic physics features ──────────────────────────────
    # TLE / GP fields differ by source; normalise common ones.
    physics_cols = {
        "EPOCH": "epoch",
        "MEAN_MOTION": "mean_motion",
        "ECCENTRICITY": "eccentricity",
        "INCLINATION": "inclination_deg",
        "RA_OF_ASC_NODE": "raan_deg",
        "ARG_OF_PERICENTER": "arg_perigee_deg",
        "MEAN_ANOMALY": "mean_anomaly_deg",
        "BSTAR": "bstar",
        "NORAD_CAT_ID": "norad_id",
        "OBJECT_NAME": "object_name",
        "OBJECT_TYPE": "object_type",
        "PERIOD": "period_min",
        "APOAPSIS": "apoapsis_km",
        "PERIAPSIS": "periapsis_km",
        "SEMIMAJOR_AXIS": "semi_major_axis_km",
    }

    rename_map = {k: v for k, v in physics_cols.items() if k in df_sat.columns}
    df = df_sat.rename(columns=rename_map)

    # Convert epoch to datetime & numeric
    if "epoch" in df.columns:
        df["epoch_dt"] = pd.to_datetime(df["epoch"], errors="coerce")
        df["epoch_unix"] = (
            df["epoch_dt"]
            .astype("int64", errors="ignore") // 10**9
        )

    # Ensure numeric types for physics columns
    numeric_fields = [
        "mean_motion", "eccentricity", "inclination_deg", "raan_deg",
        "arg_perigee_deg", "mean_anomaly_deg", "bstar",
        "period_min", "apoapsis_km", "periapsis_km", "semi_major_axis_km",
    ]
    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derived features
    if "mean_motion" in df.columns:
        # Orbital period in minutes (backup if PERIOD missing)
        if "period_min" not in df.columns:
            df["period_min"] = 1440.0 / df["mean_motion"]
        # Approximate semi-major axis via Kepler's 3rd law (km)
        # a = (mu / (2*pi*n)^2)^(1/3), mu_earth ≈ 398600.4418 km^3/s^2
        MU_EARTH = 398600.4418
        n_rad_s = df["mean_motion"] * (2 * 3.141592653589793) / 86400.0
        if "semi_major_axis_km" not in df.columns:
            df["semi_major_axis_km"] = (MU_EARTH / (n_rad_s**2)) ** (1.0 / 3.0)

    # ── NOAA: Kp index ──────────────────────────────────────
    kp_file = _latest_file("noaa_planetary_k_index_1m_*.json")
    if kp_file:
        with open(kp_file, "r", encoding="utf-8") as fh:
            kp_data = json.load(fh)
        if kp_data:
            latest_kp = kp_data[-1]  # most recent entry
            df["kp_index"] = latest_kp.get("kp_index", None)
            df["kp_timestamp"] = latest_kp.get("time_tag", None)
            logger.info(f"  Kp index → {df['kp_index'].iloc[0]}")

    # ── NOAA: Solar-wind plasma ─────────────────────────────
    plasma_file = _latest_file("noaa_solar_wind_plasma_1day_*.json")
    if plasma_file:
        with open(plasma_file, "r", encoding="utf-8") as fh:
            plasma_data = json.load(fh)
        # First row is header row — skip it
        if len(plasma_data) > 2:
            latest = plasma_data[-1]
            # Columns: [time_tag, density, speed, temperature]
            df["sw_density"] = latest[1] if len(latest) > 1 else None
            df["sw_speed"] = latest[2] if len(latest) > 2 else None
            df["sw_temperature"] = latest[3] if len(latest) > 3 else None
            logger.info(f"  Solar wind speed → {df['sw_speed'].iloc[0]} km/s")

    # ── NOAA: Magnetic field ────────────────────────────────
    mag_file = _latest_file("noaa_solar_wind_mag_1day_*.json")
    if mag_file:
        with open(mag_file, "r", encoding="utf-8") as fh:
            mag_data = json.load(fh)
        if len(mag_data) > 2:
            latest = mag_data[-1]
            # Columns: [time_tag, bx_gsm, by_gsm, bz_gsm, lon_gsm, lat_gsm, bt]
            df["bz_gsm"] = latest[3] if len(latest) > 3 else None
            df["bt"] = latest[6] if len(latest) > 6 else None
            logger.info(f"  IMF Bz → {df['bz_gsm'].iloc[0]} nT")

    logger.info(f"  Merged DataFrame shape: {df.shape}")
    return df


# ────────────────────────────────────────────────────────────
#  VERIFICATION
# ────────────────────────────────────────────────────────────
def verify_data() -> bool:
    """
    Quick health-check on the latest data files.
    Prints satellite count, Kp index, solar wind speed, and a sample.

    Returns True if all checks pass.
    """
    logger.info("=" * 60)
    logger.info("DATA VERIFICATION")
    logger.info("=" * 60)
    all_ok = True

    # ── Satellite file ──────────────────────────────────────
    sat_file = _latest_file("satellites_live_*.json")
    if sat_file and os.path.getsize(sat_file) > 0:
        with open(sat_file, "r", encoding="utf-8") as fh:
            sats = json.load(fh)
        logger.info(f"✓ Satellite file : {os.path.basename(sat_file)}")
        logger.info(f"  Satellites      : {len(sats)}")
    else:
        logger.error("✗ No valid satellite file found.")
        all_ok = False

    # ── Kp index ────────────────────────────────────────────
    kp_file = _latest_file("noaa_planetary_k_index_1m_*.json")
    if kp_file and os.path.getsize(kp_file) > 0:
        with open(kp_file, "r", encoding="utf-8") as fh:
            kp_data = json.load(fh)
        latest_kp = kp_data[-1] if kp_data else {}
        logger.info(f"✓ Kp index file   : {os.path.basename(kp_file)}")
        logger.info(f"  Latest Kp       : {latest_kp.get('kp_index', 'N/A')}")
        logger.info(f"  Kp time_tag     : {latest_kp.get('time_tag', 'N/A')}")
    else:
        logger.error("✗ No valid Kp-index file found.")
        all_ok = False

    # ── Solar wind speed ────────────────────────────────────
    plasma_file = _latest_file("noaa_solar_wind_plasma_1day_*.json")
    if plasma_file and os.path.getsize(plasma_file) > 0:
        with open(plasma_file, "r", encoding="utf-8") as fh:
            plasma = json.load(fh)
        if len(plasma) > 2:
            latest_row = plasma[-1]
            speed = latest_row[2] if len(latest_row) > 2 else "N/A"
            logger.info(f"✓ Plasma file     : {os.path.basename(plasma_file)}")
            logger.info(f"  Solar wind speed: {speed} km/s")
        else:
            logger.warning("⚠ Plasma file has too few records.")
    else:
        logger.error("✗ No valid solar-wind plasma file found.")
        all_ok = False

    # ── Merged DataFrame sample ─────────────────────────────
    df = merge_live_data()
    if not df.empty:
        sample_cols = [
            c for c in [
                "object_name", "norad_id", "epoch", "mean_motion",
                "eccentricity", "inclination_deg", "kp_index", "sw_speed",
            ] if c in df.columns
        ]
        logger.info("\n  Sample DataFrame (head):")
        print(df[sample_cols].head(10).to_string(index=False))
    else:
        logger.error("✗ Merged DataFrame is empty — check source files.")
        all_ok = False

    logger.info("-" * 60)
    logger.info(f"VERIFICATION {'PASSED ✓' if all_ok else 'FAILED ✗'}")
    logger.info("-" * 60)
    return all_ok


# ────────────────────────────────────────────────────────────
#  SCHEDULER
# ────────────────────────────────────────────────────────────
def start_scheduler() -> None:
    """
    Register collect_all() on a 10-minute cadence and block forever.
    Gracefully handles KeyboardInterrupt (Ctrl+C).
    """
    schedule.every(10).minutes.do(collect_all)
    logger.info("Scheduler armed — collecting every 10 minutes. Press Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (Ctrl+C).")


# ────────────────────────────────────────────────────────────
#  MAIN — quick test mode
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("AARAMBH_SYSTEM  —  Live Data Collector")
    logger.info("=" * 60)

    _ensure_dirs()

    # Run 2 immediate collection cycles for testing
    for cycle in range(1, 3):
        logger.info(f"\n>>> TEST CYCLE {cycle}/2")
        collect_all()
        if cycle < 2:
            logger.info("Waiting 5 seconds before next test cycle …")
            time.sleep(5)

    # Verify the collected data
    verify_data()

    # Offer to start the continuous scheduler
    logger.info("\nTest cycles complete. Starting continuous 10-min scheduler …")
    logger.info("Press Ctrl+C at any time to exit.\n")
    start_scheduler()
