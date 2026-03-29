"""
load.py — Load cleaned data into PostgreSQL.

Uses SQLAlchemy to create the orders table and bulk-insert cleaned data.
"""

import logging
import os

import pandas as pd
from sqlalchemy import text

# Add parent directory to path so we can import config
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.db_config import get_engine

logger = logging.getLogger(__name__)

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def create_tables(engine):
    """Run create_tables.sql to set up the database schema."""
    sql_file = os.path.join(SQL_DIR, "create_tables.sql")
    logger.info(f"Creating tables from: {sql_file}")

    with open(sql_file, "r") as f:
        sql = f.read()

    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    logger.info("Tables created successfully")


def load_data(df, engine):
    """
    Load DataFrame into PostgreSQL orders table.

    Args:
        df: Cleaned DataFrame from transform step.
        engine: SQLAlchemy engine.
    """
    logger.info(f"Loading {len(df)} rows into PostgreSQL...")

    # Ensure correct column order
    columns = [
        "order_id", "order_date", "customer_id", "product",
        "category", "price", "quantity", "total_price"
    ]
    df = df[columns]

    # Load using pandas to_sql
    df.to_sql(
        name="orders",
        con=engine,
        if_exists="append",   # Append to the table we just created with exact schema
        index=False,
        method="multi",       # Batch insert for performance
        chunksize=500,
    )

    # Verify row count
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM orders"))
        count = result.scalar()

    logger.info(f"Successfully loaded {count} rows into 'orders' table")
    return count


def load(df):
    """
    Main load function: create tables + insert data.

    Args:
        df: Cleaned DataFrame from transform step.

    Returns:
        int: Number of rows loaded.
    """
    engine = get_engine()

    # Create schema
    create_tables(engine)

    # Load data
    count = load_data(df, engine)

    engine.dispose()
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from extract import extract
    from transform import transform

    raw_df = extract()
    cleaned_df = transform(raw_df)
    load(cleaned_df)
