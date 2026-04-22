"""
ORBITRON-UNIFIED — Live Data Preprocessor & Visualizer
======================================================
Layer 2 of the ingestion pipeline.  Reads raw JSON snapshots produced by
data_collector.py, engineers orbital-physics features, fuses satellite +
space-weather data into a single enriched DataFrame, and renders an
interactive Plotly dashboard saved as HTML.

Inputs  : data/satellites_live_*.json, data/noaa_*.json
Outputs : data/processed_live_data.parquet, data/live_preview.html

Author  : ORBITRON Team
Tested  : Windows 11 / Python 3.10+
"""

import os
import sys
import glob
import json
import math
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ────────────────────────────────────────────────────────────
#  CONFIGURATION
# ────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# Physical constants
MU_EARTH = 398600.4418       # Earth gravitational parameter [km³/s²]
R_EARTH  = 6371.0            # Mean Earth radius [km]
PI       = math.pi
TWO_PI   = 2.0 * PI

# ────────────────────────────────────────────────────────────
#  LOGGING  (Windows cp1252-safe stream handler)
# ────────────────────────────────────────────────────────────
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
# Prevent UnicodeEncodeError on Windows cp1252 consoles
if hasattr(_handler.stream, "reconfigure"):
    _handler.stream.reconfigure(errors="replace")
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger("orbitron.preprocessor")


# ────────────────────────────────────────────────────────────
#  HELPER — find the newest file matching a glob pattern
# ────────────────────────────────────────────────────────────
def get_latest_file(pattern: str) -> str | None:
    """
    Return the most-recently modified file in DATA_DIR matching *pattern*.
    Returns None if no match found.

    Example:
        get_latest_file("satellites_live_*.json")
    """
    files = glob.glob(os.path.join(DATA_DIR, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


# ────────────────────────────────────────────────────────────
#  CORE — merge raw JSON into an enriched DataFrame
# ────────────────────────────────────────────────────────────
def merge_live_data() -> pd.DataFrame:
    """
    Load the latest CelesTrak satellite file + NOAA space-weather files,
    engineer orbital-physics features, and broadcast the current
    space-weather state into every satellite row.

    Returns
    -------
    pd.DataFrame  with columns:
        ── identity ──
        object_name, object_id, norad_cat_id, classification_type,
        ── raw Keplerian ──
        epoch, mean_motion, eccentricity, inclination, raan, arg_pericenter,
        mean_anomaly, bstar, mean_motion_dot, mean_motion_ddot,
        ephemeris_type, element_set_no, rev_at_epoch,
        ── engineered physics ──
        semi_major_axis_km, orbital_period_min, perigee_altitude_km,
        apogee_altitude_km, approx_velocity_kms,
        ── space-weather overlay ──
        current_kp_index, solar_wind_speed_kms, timestamp
    """
    logger.info("=" * 60)
    logger.info("MERGE LIVE DATA — building enriched DataFrame")
    logger.info("=" * 60)

    # ── 1. Load satellite GP elements ───────────────────────
    sat_file = get_latest_file("satellites_live_*.json")
    if sat_file is None:
        logger.error("No satellite file found — run data_collector.py first.")
        return pd.DataFrame()

    with open(sat_file, "r", encoding="utf-8") as fh:
        sat_raw = json.load(fh)
    logger.info(f"  Loaded {len(sat_raw)} satellites from {os.path.basename(sat_file)}")
    logger.info(f"  Satellite columns: {list(sat_raw[0].keys())}")

    # Build DataFrame and normalise column names to snake_case
    df = pd.DataFrame(sat_raw)
    df.columns = [c.lower() for c in df.columns]

    # Rename for clarity
    rename_map = {
        "ra_of_asc_node":   "raan",
        "arg_of_pericenter": "arg_pericenter",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns},
              inplace=True)

    # ── 2. Cast numeric columns ─────────────────────────────
    numeric_cols = [
        "mean_motion", "eccentricity", "inclination", "raan",
        "arg_pericenter", "mean_anomaly", "bstar",
        "mean_motion_dot", "mean_motion_ddot",
        "norad_cat_id", "element_set_no", "rev_at_epoch",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert epoch string → datetime
    if "epoch" in df.columns:
        df["epoch_dt"] = pd.to_datetime(df["epoch"], errors="coerce", utc=True)

    # ── 3. Engineer orbital-physics features ────────────────
    logger.info("  Engineering physics features ...")

    #  Mean motion [rev/day] → angular rate [rad/s]
    n_rad_s = df["mean_motion"] * TWO_PI / 86400.0

    #  Semi-major axis via Kepler's third law:  a = (μ / n²)^(1/3)
    df["semi_major_axis_km"] = np.where(
        n_rad_s > 0,
        (MU_EARTH / (n_rad_s ** 2)) ** (1.0 / 3.0),
        np.nan,
    )

    #  Orbital period [minutes] = 2π / n  (convert s → min)
    df["orbital_period_min"] = np.where(
        n_rad_s > 0,
        TWO_PI / n_rad_s / 60.0,
        np.nan,
    )

    #  Perigee altitude [km] = a(1 − e) − R_earth
    df["perigee_altitude_km"] = (
        df["semi_major_axis_km"] * (1.0 - df["eccentricity"]) - R_EARTH
    )

    #  Apogee altitude [km] = a(1 + e) − R_earth
    df["apogee_altitude_km"] = (
        df["semi_major_axis_km"] * (1.0 + df["eccentricity"]) - R_EARTH
    )

    #  Approximate circular-equivalent velocity [km/s] = √(μ / a)
    df["approx_velocity_kms"] = np.where(
        df["semi_major_axis_km"] > 0,
        np.sqrt(MU_EARTH / df["semi_major_axis_km"]),
        np.nan,
    )

    # ── 4. Load NOAA Kp index ───────────────────────────────
    current_kp = np.nan
    kp_file = get_latest_file("noaa_planetary_k_index_1m_*.json")
    if kp_file:
        try:
            with open(kp_file, "r", encoding="utf-8") as fh:
                kp_data = json.load(fh)
            if kp_data:
                latest_kp = kp_data[-1]
                current_kp = float(latest_kp.get("kp_index", np.nan))
                logger.info(
                    f"  Kp index = {current_kp}  "
                    f"(time_tag: {latest_kp.get('time_tag', 'N/A')})"
                )
        except (json.JSONDecodeError, ValueError, KeyError) as exc:
            logger.warning(f"  Could not parse Kp file: {exc}")
    else:
        logger.warning("  No Kp-index file found — using NaN.")

    df["current_kp_index"] = current_kp

    # ── 5. Load NOAA solar-wind plasma ──────────────────────
    sw_speed = np.nan
    plasma_file = get_latest_file("noaa_solar_wind_plasma_1day_*.json")
    if plasma_file:
        try:
            with open(plasma_file, "r", encoding="utf-8") as fh:
                plasma_data = json.load(fh)
            # Row 0 is header: ["time_tag", "density", "speed", "temperature"]
            # Subsequent rows are data arrays
            if len(plasma_data) > 1:
                latest_row = plasma_data[-1]
                sw_speed = float(latest_row[2]) if latest_row[2] is not None else np.nan
                logger.info(f"  Solar wind speed = {sw_speed} km/s")
        except (json.JSONDecodeError, ValueError, IndexError) as exc:
            logger.warning(f"  Could not parse plasma file: {exc}")
    else:
        logger.warning("  No solar-wind plasma file found — using NaN.")

    df["solar_wind_speed_kms"] = sw_speed

    # ── 6. Add processing timestamp ─────────────────────────
    df["timestamp"] = datetime.now(timezone.utc).isoformat()

    # ── 7. Summary stats ────────────────────────────────────
    logger.info("-" * 60)
    logger.info(f"  Final DataFrame shape   : {df.shape}")
    logger.info(f"  Columns                 : {list(df.columns)}")
    logger.info(f"  Semi-major axis range   : "
                f"{df['semi_major_axis_km'].min():.1f} - "
                f"{df['semi_major_axis_km'].max():.1f} km")
    logger.info(f"  Perigee altitude range  : "
                f"{df['perigee_altitude_km'].min():.1f} - "
                f"{df['perigee_altitude_km'].max():.1f} km")
    logger.info(f"  Approx velocity range   : "
                f"{df['approx_velocity_kms'].min():.2f} - "
                f"{df['approx_velocity_kms'].max():.2f} km/s")
    logger.info("-" * 60)

    return df


# ────────────────────────────────────────────────────────────
#  VISUALISATION — interactive Plotly dashboard
# ────────────────────────────────────────────────────────────
def visualize_live_data(df: pd.DataFrame | None = None) -> str:
    """
    Render a 3-panel Plotly dashboard and save as HTML.

    Panels
    ------
    1. 3D Scatter : Inclination × Eccentricity × Semi-major Axis
                    coloured by Kp-based risk level.
    2. Gauge      : Current planetary Kp index (0 → 9 scale).
    3. Line chart : Recent solar-wind speed time-series (from plasma JSON).

    Returns the path to the saved HTML file.
    """
    if df is None or df.empty:
        df = merge_live_data()
    if df.empty:
        logger.error("No data to visualize.")
        return ""

    logger.info("Building Plotly dashboard ...")

    # ── Colour scheme ───────────────────────────────────────
    #  Kp risk buckets:  0-2 → LOW (green), 3-4 → MODERATE (gold),
    #                    5-6 → HIGH (orange), 7-9 → SEVERE (red)
    kp = df["current_kp_index"].iloc[0] if not df["current_kp_index"].isna().all() else 0

    def _kp_risk_color(kp_val):
        """Map a Kp value to a named risk tier + colour."""
        if kp_val <= 2:
            return "Low",       "#00c853"   # green
        elif kp_val <= 4:
            return "Moderate",  "#ffd600"   # gold
        elif kp_val <= 6:
            return "High",      "#ff6d00"   # orange
        else:
            return "Severe",    "#d50000"   # red

    risk_label, risk_color = _kp_risk_color(kp)

    # ── Assign per-satellite Kp risk colour for the scatter ─
    # All satellites share the same Kp right now, but we'll still
    # generate scalar values so the colorbar is meaningful.
    df["kp_risk_score"] = df["current_kp_index"].fillna(0)

    # ── Load solar-wind time-series for the line chart ──────
    plasma_file = get_latest_file("noaa_solar_wind_plasma_1day_*.json")
    sw_times, sw_speeds = [], []
    if plasma_file:
        try:
            with open(plasma_file, "r", encoding="utf-8") as fh:
                plasma_raw = json.load(fh)
            # Skip header row [0]; each data row = [time_tag, density, speed, temp]
            for row in plasma_raw[1:]:
                try:
                    t = pd.to_datetime(row[0])
                    s = float(row[2]) if row[2] is not None else np.nan
                    sw_times.append(t)
                    sw_speeds.append(s)
                except (ValueError, TypeError, IndexError):
                    continue
        except (json.JSONDecodeError, OSError):
            pass

    # ── Build figure with custom grid ───────────────────────
    #  Row 1: 3D scatter (spans full width)
    #  Row 2: Gauge (left) + Solar wind line (right)
    fig = make_subplots(
        rows=2, cols=2,
        specs=[
            [{"type": "scene", "colspan": 2}, None],
            [{"type": "indicator"}, {"type": "xy"}],
        ],
        row_heights=[0.60, 0.40],
        column_widths=[0.35, 0.65],
        subplot_titles=[
            "Orbital Elements — 3D Scatter",
            "",
            "Solar Wind Speed (Last 24 h)",
        ],
        vertical_spacing=0.08,
        horizontal_spacing=0.08,
    )

    # ── Panel 1: 3D Scatter ─────────────────────────────────
    fig.add_trace(
        go.Scatter3d(
            x=df["inclination"],
            y=df["eccentricity"],
            z=df["semi_major_axis_km"],
            mode="markers",
            marker=dict(
                size=3,
                color=df["kp_risk_score"],
                colorscale=[
                    [0.0, "#00c853"],    # Kp 0 — green
                    [0.33, "#ffd600"],   # Kp 3 — gold
                    [0.66, "#ff6d00"],   # Kp 6 — orange
                    [1.0, "#d50000"],    # Kp 9 — red
                ],
                cmin=0,
                cmax=9,
                colorbar=dict(
                    title="Kp Index",
                    len=0.55,
                    y=0.75,
                    thickness=15,
                ),
                opacity=0.85,
            ),
            hovertext=[
                f"{row.object_name}<br>"
                f"NORAD: {int(row.norad_cat_id)}<br>"
                f"a = {row.semi_major_axis_km:.1f} km<br>"
                f"e = {row.eccentricity:.6f}<br>"
                f"i = {row.inclination:.2f} deg<br>"
                f"v ~ {row.approx_velocity_kms:.2f} km/s"
                for _, row in df.iterrows()
            ],
            hoverinfo="text",
            name="Satellites",
        ),
        row=1, col=1,
    )

    # Style the 3D axes
    fig.update_scenes(
        xaxis_title="Inclination [deg]",
        yaxis_title="Eccentricity",
        zaxis_title="Semi-major Axis [km]",
        xaxis=dict(backgroundcolor="#1a1a2e", gridcolor="#333355"),
        yaxis=dict(backgroundcolor="#1a1a2e", gridcolor="#333355"),
        zaxis=dict(backgroundcolor="#1a1a2e", gridcolor="#333355"),
        bgcolor="#0f0f23",
        camera=dict(eye=dict(x=1.6, y=1.6, z=0.9)),
    )

    # ── Panel 2: Kp Gauge ───────────────────────────────────
    fig.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=kp,
            number=dict(font=dict(size=48, color=risk_color)),
            title=dict(
                text=f"Kp Index -- <b>{risk_label}</b>",
                font=dict(size=16, color="#e0e0e0"),
            ),
            delta=dict(reference=3, increasing_color="#d50000",
                       decreasing_color="#00c853"),
            gauge=dict(
                axis=dict(range=[0, 9], tickwidth=2, tickcolor="#555"),
                bar=dict(color=risk_color, thickness=0.6),
                bgcolor="#1a1a2e",
                borderwidth=2,
                bordercolor="#333355",
                steps=[
                    dict(range=[0, 2], color="#1b3a1b"),    # quiet
                    dict(range=[2, 4], color="#3a3a1b"),    # unsettled
                    dict(range=[4, 6], color="#3a2a1b"),    # active
                    dict(range=[6, 9], color="#3a1b1b"),    # storm
                ],
                threshold=dict(
                    line=dict(color="#ffffff", width=3),
                    thickness=0.8,
                    value=kp,
                ),
            ),
        ),
        row=2, col=1,
    )

    # ── Panel 3: Solar Wind Speed Line Chart ────────────────
    if sw_times and sw_speeds:
        fig.add_trace(
            go.Scatter(
                x=sw_times,
                y=sw_speeds,
                mode="lines",
                line=dict(color="#00e5ff", width=1.5),
                fill="tozeroy",
                fillcolor="rgba(0,229,255,0.08)",
                name="Solar Wind Speed",
                hovertemplate="<b>%{x}</b><br>Speed: %{y:.0f} km/s<extra></extra>",
            ),
            row=2, col=2,
        )

        # Solar wind reference thresholds added as layout shapes
        # (using add_hline with row/col fails on mixed subplot grids)
        # xref/yref for row=2, col=2 are "x2" / "y2" in a 2x2 grid
        for y_val, color, label in [
            (400, "#ffd600", "400 km/s (Normal)"),
            (600, "#ff6d00", "600 km/s (Elevated)"),
        ]:
            fig.add_shape(
                type="line",
                xref="x2", yref="y2",
                x0=sw_times[0], x1=sw_times[-1],
                y0=y_val, y1=y_val,
                line=dict(color=color, width=1.5, dash="dash"),
            )
            fig.add_annotation(
                xref="x2", yref="y2",
                x=sw_times[-1], y=y_val,
                text=label,
                showarrow=False,
                font=dict(color=color, size=10),
                xanchor="right",
                yshift=10,
            )

    # Style axis for the solar wind chart  (xaxis2/yaxis2 for row2,col2)
    fig.update_xaxes(
        title_text="UTC Time",
        gridcolor="#333355",
        row=2, col=2,
    )
    fig.update_yaxes(
        title_text="Speed [km/s]",
        gridcolor="#333355",
        row=2, col=2,
    )

    # ── Global layout ───────────────────────────────────────
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    fig.update_layout(
        title=dict(
            text=(
                f"<b>ORBITRON-UNIFIED Live Data Preview</b>"
                f"<br><sup style='color:#888'>"
                f"{len(df)} satellites · Kp = {kp:.0f} ({risk_label}) · "
                f"SW = {df['solar_wind_speed_kms'].iloc[0]:.0f} km/s · "
                f"Generated {now_str}</sup>"
            ),
            x=0.5,
            font=dict(size=22, color="#e0e0e0"),
        ),
        template="plotly_dark",
        paper_bgcolor="#0f0f23",
        plot_bgcolor="#1a1a2e",
        font=dict(family="Inter, Segoe UI, Roboto, sans-serif", color="#ccc"),
        height=950,
        margin=dict(l=40, r=40, t=100, b=40),
        showlegend=False,
    )

    # ── Save to disk ────────────────────────────────────────
    html_path = os.path.join(DATA_DIR, "live_preview.html")
    fig.write_html(
        html_path,
        include_plotlyjs="cdn",      # keep file size small
        full_html=True,
        config={"displayModeBar": True, "scrollZoom": True},
    )
    logger.info(f"  Dashboard saved -> {html_path}")

    return html_path


# ────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("ORBITRON-UNIFIED — Data Preprocessor")
    logger.info("=" * 60)

    # 1. Merge & engineer features
    df = merge_live_data()

    if df.empty:
        logger.error("Pipeline produced empty DataFrame — aborting.")
        sys.exit(1)

    # 2. Save enriched data as Parquet (compact, typed, fast)
    parquet_path = os.path.join(DATA_DIR, "processed_live_data.parquet")

    # Drop datetime columns that may cause parquet issues, keep ISO string
    save_df = df.copy()
    if "epoch_dt" in save_df.columns:
        save_df["epoch_dt"] = save_df["epoch_dt"].astype(str)

    save_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    parquet_size = os.path.getsize(parquet_path) / 1024
    logger.info(f"  Parquet saved -> {parquet_path} ({parquet_size:.1f} KB)")

    # 3. Visualize
    html_path = visualize_live_data(df)

    # 4. Done!
    logger.info("")
    print("\n[OK] Preprocessing complete! Open data/live_preview.html in browser")
    logger.info(f"   Parquet : {parquet_path}")
    logger.info(f"   HTML    : {html_path}")
    logger.info("")
    logger.info("")

    # Print a quick sample
    sample_cols = [c for c in [
        "object_name", "norad_cat_id", "semi_major_axis_km",
        "perigee_altitude_km", "apogee_altitude_km", "approx_velocity_kms",
        "current_kp_index", "solar_wind_speed_kms",
    ] if c in df.columns]
    print("\n📊 Sample (first 10 rows):\n")
    print(df[sample_cols].head(10).to_string(index=False))
