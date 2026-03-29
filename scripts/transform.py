"""
transform.py — Clean and transform dirty e-commerce data.

Handles:
  1. Remove duplicate rows
  2. Remove rows with missing values
  3. Standardize date format → YYYY-MM-DD
  4. Clean category names → lowercase + fix typos
  5. Clean price → strip symbols, convert to float
  6. Remove invalid rows (negative price or quantity)
  7. Calculate total_price = price × quantity
  8. Save cleaned data to processed folder
"""

import logging
import os
import re
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(
    os.path.dirname(__file__), "..", "data", "processed"
)
PROCESSED_FILE = os.path.join(PROCESSED_DIR, "orders_cleaned.csv")

# Mapping for category typo corrections
CATEGORY_MAPPING = {
    "electronics": "electronics",
    "electronicss": "electronics",
    "electronic": "electronics",
    "clothing": "clothing",
    "clothng": "clothing",
    "clothes": "clothing",
    "books": "books",
    "book": "books",
    "home": "home",
    "home & garden": "home",
    "home_goods": "home",
    "sports": "sports",
    "sport": "sports",
}


def remove_duplicates(df):
    """Remove duplicate rows."""
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    removed = before - after
    logger.info(f"Removed {removed} duplicate rows ({before} → {after})")
    return df


def remove_missing_values(df):
    """Remove rows with any missing or empty values."""
    before = len(df)
    # Replace empty strings with NaN, then drop
    df = df.replace("", pd.NA)
    df = df.dropna()
    after = len(df)
    removed = before - after
    logger.info(f"Removed {removed} rows with missing values ({before} → {after})")
    return df


def standardize_dates(df):
    """
    Standardize order_date to YYYY-MM-DD format.

    Handles formats:
      - 2024-01-15   (YYYY-MM-DD)
      - 15/01/2024   (DD/MM/YYYY)
      - 15-01-2024   (DD-MM-YYYY)
      - 01/15/2024   (MM/DD/YYYY)
      - Jan 15, 2024 (Mon DD, YYYY)
    """
    date_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%b %d, %Y",
    ]

    def parse_date(date_str):
        if pd.isna(date_str) or str(date_str).strip() == "":
            return pd.NaT
        
        if isinstance(date_str, datetime):
             return date_str
             
        date_str = str(date_str).strip()
        date_str = date_str.strip()
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        logger.warning(f"Could not parse date: {date_str}")
        return pd.NaT

    df["order_date"] = df["order_date"].apply(parse_date)

    # Drop rows where date could not be parsed
    unparsed = df["order_date"].isna().sum()
    if unparsed > 0:
        logger.warning(f"Dropping {unparsed} rows with unparseable dates")
        df = df.dropna(subset=["order_date"])

    logger.info("Standardized all dates to datetime64 format")

    return df


def clean_categories(df):
    """Standardize category names: lowercase + fix typos."""
    df["category"] = df["category"].str.strip().str.lower()
    df["category"] = df["category"].map(CATEGORY_MAPPING).fillna(df["category"])

    unique_cats = df["category"].unique()
    logger.info(f"Cleaned categories → {len(unique_cats)} unique: {sorted(unique_cats)}")

    return df


def clean_prices(df):
    """
    Clean price column: strip currency symbols, convert to float.

    Handles: $100.00, USD 50.00, 1,234.56, plain numbers.
    """
    def parse_price(price_str):
        if pd.isna(price_str) or str(price_str).strip() == "":
            return None
        if isinstance(price_str, (int, float)):
            return float(price_str)
            
        # Remove $, USD, commas, whitespace
        cleaned = str(price_str).strip()
        cleaned = re.sub(r"[USD$,]", "", cleaned)
        cleaned = cleaned.strip()
        try:
            return float(cleaned)
        except ValueError:
            logger.warning(f"Could not parse price: {price_str}")
            return None

    df["price"] = df["price"].apply(parse_price)

    unparsed = df["price"].isna().sum()
    if unparsed > 0:
        logger.warning(f"Dropping {unparsed} rows with unparseable prices")
        df = df.dropna(subset=["price"])

    logger.info("Cleaned price column → numeric float")
    return df


def remove_invalid_rows(df):
    """Remove rows with negative or zero price/quantity."""
    before = len(df)

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df.dropna(subset=["quantity"])
    df = df[df["price"] > 0]
    df = df[df["quantity"] > 0]

    after = len(df)
    removed = before - after
    logger.info(f"Removed {removed} invalid rows (negative/zero price or quantity)")

    return df


def calculate_total_price(df):
    """Add total_price = price × quantity."""
    df["total_price"] = (df["price"] * df["quantity"]).round(2)
    logger.info(f"Calculated total_price — min: {df['total_price'].min()}, max: {df['total_price'].max()}")
    return df


def save_cleaned_data(df):
    """Save cleaned DataFrame to CSV."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(PROCESSED_FILE, index=False)
    logger.info(f"Saved cleaned data → {PROCESSED_FILE} ({len(df)} rows)")


def transform(df):
    """
    Run all transformation steps.

    Args:
        df: Raw DataFrame from extract step.

    Returns:
        pd.DataFrame: Cleaned and transformed data.
    """
    initial_rows = len(df)
    logger.info(f"Starting transformation — {initial_rows} rows")
    logger.info("=" * 50)

    # Step 1: Remove duplicates
    df = remove_duplicates(df)

    # Step 2: Remove missing values
    df = remove_missing_values(df)

    # Step 3: Standardize dates
    df = standardize_dates(df)

    # Step 4: Clean categories
    df = clean_categories(df)

    # Step 5: Clean prices
    df = clean_prices(df)

    # Step 6: Remove invalid rows
    df = remove_invalid_rows(df)

    # Step 7: Calculate total_price
    df = calculate_total_price(df)

    # Step 8: Ensure correct data types
    df["quantity"] = df["quantity"].astype(int)

    # Step 9: Save cleaned data
    save_cleaned_data(df)

    final_rows = len(df)
    removed_total = initial_rows - final_rows
    logger.info("=" * 50)
    logger.info(
        f"Transformation complete: {initial_rows} → {final_rows} rows "
        f"({removed_total} removed, {removed_total / initial_rows * 100:.1f}%)"
    )

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from extract import extract
    raw_df = extract()
    cleaned_df = transform(raw_df)
    print(cleaned_df.head(10))
    print(f"\nFinal shape: {cleaned_df.shape}")
