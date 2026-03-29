"""
test_pipeline.py — Integration tests for the full ETL pipeline.

Tests the end-to-end flow from Extraction to Transformation (Load is skipped or mocked 
to avoid side effects during automated testing).
"""

import pandas as pd
import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.etl_pipeline import run_pipeline

def test_pipeline_returns_clean_dataframe_with_valid_schema():
    """
    Integration test for the full ETL pipeline flow (Extract -> Transform).
    This test verifies that the pipeline correctly processes raw data into a 
    clean, structured format suitable for a Data Warehouse without 
    connecting to a live database.
    
    Why this matters:
    Ensures the 'Production Mindset' by validating that the pipeline output 
    is deterministic and meets high-quality data standards before loading.
    """
    
    # Run the pipeline in 'skip_load' mode to avoid DB side effects in CI environments
    df = run_pipeline(skip_load=True)
    
    # 1. Row Count Validation
    # Ensures that the extraction and transformation steps actually produce data.
    # A failure here indicates a critical failure in source reading or filter logic.
    assert isinstance(df, pd.DataFrame), "Pipeline output should be a pandas DataFrame."
    assert len(df) > 0, "The cleaned DataFrame is empty. Check source data or filtering logic."
    
    # 2. Strict Schema and Column Order Validation
    # Data Warehouse schemas must be deterministic. This ensures downstream 
    # analytics and BI tools don't break due to unexpected schema changes.
    expected_columns = [
        "order_id",
        "order_date",
        "customer_id",
        "product",
        "category",
        "price",
        "quantity",
        "total_price",
    ]
    assert list(df.columns) == expected_columns, (
        f"Schema mismatch! Expected: {expected_columns}, but got: {list(df.columns)}"
    )

    # 3. Missing Value (Null) Check
    # Ensures that the transformation step removed all missing values.
    # Clean analytical datasets must not contain nulls in required fields to 
    # maintain the integrity of business reports.
    assert not df.isnull().any().any(), (
        "Cleaned dataset contains null values. The 'Automated Cleaning' claim failed."
    )

    # 4. Business Logic: Numeric Integrity
    # Verifies that invalid data (negative prices/quantities) was correctly filtered.
    assert (df["price"] > 0).all(), "Found invalid price values (<= 0)."
    assert (df["quantity"] > 0).all(), "Found invalid quantity values (<= 0)."
    
    # 5. Business Logic: Deduplication
    # Ensures that exact duplicate records are removed to prevent revenue overestimation.
    assert df.duplicated().sum() == 0, "Deduplication failed; exact duplicates remain in the dataset."

    # 6. Business Logic: total_price correctness
    # Ensures feature engineering step is mathematically correct.
    recalculated_total = (df["price"] * df["quantity"]).round(2)
    assert (df["total_price"] == recalculated_total).all(), (
        "Column 'total_price' must equal price * quantity for every row."
    )

    # 7. Data Type Validation: order_date must be datetime
    # Ensures the dataset is analytics-ready for time-based queries.
    assert pd.api.types.is_datetime64_any_dtype(df["order_date"]), (
        "'order_date' column must be datetime for analytics readiness."
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])