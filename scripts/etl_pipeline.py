"""
etl_pipeline.py — Orchestrate the full ETL pipeline.

Runs Extract → Transform → Load with logging and timing.

Usage:
    python scripts/etl_pipeline.py
"""

import logging
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load

# ---- Logging Setup ----

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
LOG_FILE = os.path.join(LOG_DIR, "etl.log")


def setup_logging():
    """Configure logging to both console and file."""
    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)-20s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler
    file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def run_pipeline(skip_load: bool = False):
    """
    Execute the full ETL pipeline.
    
    Args:
        skip_load (bool): If True, the data loading step to PostgreSQL will be skipped.
                          Useful for CI/CD and integration testing.
    """
    logger = logging.getLogger("pipeline")
    pipeline_start = time.time()

    logger.info("=" * 60)
    logger.info("   E-COMMERCE SALES DATA PIPELINE")
    logger.info("=" * 60)

    try:
        # ---- STEP 1: EXTRACT ----
        logger.info("")
        logger.info("📥 STEP 1: EXTRACT")
        logger.info("-" * 40)
        step_start = time.time()

        raw_df = extract()

        extract_time = time.time() - step_start
        logger.info(f"⏱  Extract completed in {extract_time:.2f}s")

        # ---- STEP 2: TRANSFORM ----
        logger.info("")
        logger.info("🔧 STEP 2: TRANSFORM")
        logger.info("-" * 40)
        step_start = time.time()

        cleaned_df = transform(raw_df)

        transform_time = time.time() - step_start
        logger.info(f"⏱  Transform completed in {transform_time:.2f}s")

        # ---- STEP 3: LOAD ----
        rows_loaded = 0
        load_time = 0

        if not skip_load:
            logger.info("")
            logger.info("📤 STEP 3: LOAD")
            logger.info("-" * 40)
            step_start = time.time()

            rows_loaded = load(cleaned_df)

            load_time = time.time() - step_start
            logger.info(f"⏱  Load completed in {load_time:.2f}s")
        else:
            logger.info("")
            logger.info("⏩ STEP 3: LOAD (Skipped by user request)")


        # ---- SUMMARY ----
        total_time = time.time() - pipeline_start
        logger.info("")
        logger.info("=" * 60)
        logger.info("   PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"   Raw rows extracted : {len(raw_df)}")
        logger.info(f"   Cleaned rows       : {len(cleaned_df)}")
        logger.info(f"   Rows loaded to DB  : {rows_loaded}")
        logger.info(f"   Rows removed       : {len(raw_df) - len(cleaned_df)}")
        logger.info(f"   Extract time       : {extract_time:.2f}s")
        logger.info(f"   Transform time     : {transform_time:.2f}s")
        logger.info(f"   Load time          : {load_time:.2f}s")
        logger.info(f"   Total time         : {total_time:.2f}s")
        logger.info("=" * 60)
        logger.info("✅ Pipeline completed successfully!")
        return cleaned_df

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    setup_logging()
    run_pipeline()
