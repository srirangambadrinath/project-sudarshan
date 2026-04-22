import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime

# Append project root to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import data_ingestion.data_collector as data_collector
import data_ingestion.data_preprocessor as data_preprocessor
import models.sudarshan_ml_engine as sudarshan_ml_engine

# Setup explicit logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("sudarshan_pipeline")

def run_preprocessor():
    """
    Import and run the data preprocessor steps to create the latest parquet
    using the functional methods in data_preprocessor.py
    """
    df = data_preprocessor.merge_live_data()
    if df.empty:
        logger.error("Pipeline produced empty DataFrame — aborting.")
        sys.exit(1)

    DATA_DIR = data_preprocessor.DATA_DIR
    parquet_path = os.path.join(DATA_DIR, "processed_live_data.parquet")

    save_df = df.copy()
    if "epoch_dt" in save_df.columns:
        save_df["epoch_dt"] = save_df["epoch_dt"].astype(str)

    # Save to parquet
    save_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"Parquet saved -> {parquet_path}")

    # Generate live preview
    html_path = data_preprocessor.visualize_live_data(df)
    logger.info(f"HTML saved -> {html_path}")

def main():
    logger.info("=" * 80)
    logger.info(f"🚀 PROJECT SUDARSHAN FULL PIPELINE INIT - {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    # 1. Collect live data
    logger.info("\n[1/3] Refreshing Live Satellite & Space Weather Data...")
    try:
        data_collector.collect_all()
    except Exception as e:
        logger.error(f"Error during data collection: {e}")
        
    # 2. Preprocess data
    logger.info("\n[2/3] Executing Data Preprocessor (Merging & Physics Features)...")
    try:
        run_preprocessor()
    except Exception as e:
        logger.error(f"Error during preprocessing: {e}")
        
    # 3. Model Engine
    logger.info("\n[3/3] Running SUDARSHAN ML Engine (MLP + Transformer + SHAP)...")
    try:
        sudarshan_ml_engine.main()
    except Exception as e:
        logger.error(f"Error during model execution: {e}")

    logger.info("=" * 80)
    logger.info("✅ PROJECT SUDARSHAN FULL PIPELINE COMPLETE! Open data/sudarshan_risk_map.html")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
