"""
extract.py — Extract raw data from CSV file.

Reads the dirty orders dataset and returns a pandas DataFrame.
"""

import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

RAW_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "raw", "orders.csv"
)


def extract(file_path=None):
    """
    Extract data from CSV file.

    Args:
        file_path: Path to the CSV file. Defaults to data/raw/orders.csv.

    Returns:
        pd.DataFrame: Raw data.
    """
    path = file_path or RAW_DATA_PATH
    logger.info(f"Extracting data from: {path}")

    if not os.path.exists(path):
        logger.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(path, dtype=str)  # Read everything as string first

    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Columns: {list(df.columns)}")

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = extract()
    print(df.head(10))
    print(f"\nShape: {df.shape}")
