"""
test_transform.py — Unit tests for the transform module.

Tests each data cleaning function independently using small test DataFrames.
"""

import pandas as pd
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from transform import (
    remove_duplicates,
    remove_missing_values,
    standardize_dates,
    clean_categories,
    clean_prices,
    remove_invalid_rows,
    calculate_total_price,
)


class TestRemoveDuplicates:
    """Tests for duplicate removal."""

    def test_removes_exact_duplicates(self):
        df = pd.DataFrame({
            "order_id": ["ORD-0001", "ORD-0002", "ORD-0001"],
            "product": ["Mouse", "Keyboard", "Mouse"],
            "price": ["10.00", "20.00", "10.00"],
        })
        result = remove_duplicates(df)
        assert len(result) == 2

    def test_keeps_unique_rows(self):
        df = pd.DataFrame({
            "order_id": ["ORD-0001", "ORD-0002", "ORD-0003"],
            "product": ["Mouse", "Keyboard", "Monitor"],
        })
        result = remove_duplicates(df)
        assert len(result) == 3


class TestRemoveMissingValues:
    """Tests for missing value handling."""

    def test_removes_empty_strings(self):
        df = pd.DataFrame({
            "order_id": ["ORD-0001", "ORD-0002", "ORD-0003"],
            "product": ["Mouse", "", "Monitor"],
            "price": ["10.00", "20.00", "30.00"],
        })
        result = remove_missing_values(df)
        assert len(result) == 2

    def test_removes_nan(self):
        df = pd.DataFrame({
            "order_id": ["ORD-0001", "ORD-0002"],
            "product": ["Mouse", None],
            "price": ["10.00", "20.00"],
        })
        result = remove_missing_values(df)
        assert len(result) == 1


class TestStandardizeDates:
    """Tests for date standardization."""

    def test_standard_format(self):
        df = pd.DataFrame({"order_date": ["2024-01-15"]})
        result = standardize_dates(df)
        res_date = result["order_date"].iloc[0]
        assert res_date.year == 2024
        assert res_date.month == 1
        assert res_date.day == 15

    def test_dd_slash_mm_yyyy(self):
        df = pd.DataFrame({"order_date": ["15/01/2024"]})
        result = standardize_dates(df)
        res_date = result["order_date"].iloc[0]
        assert res_date.year == 2024
        assert res_date.month == 1
        assert res_date.day == 15

    def test_dd_dash_mm_yyyy(self):
        df = pd.DataFrame({"order_date": ["15-01-2024"]})
        result = standardize_dates(df)
        res_date = result["order_date"].iloc[0]
        assert res_date.year == 2024
        assert res_date.month == 1
        assert res_date.day == 15

    def test_month_name_format(self):
        df = pd.DataFrame({"order_date": ["Jan 15, 2024"]})
        result = standardize_dates(df)
        res_date = result["order_date"].iloc[0]
        assert res_date.year == 2024
        assert res_date.month == 1
        assert res_date.day == 15

    def test_drops_unparseable(self):
        df = pd.DataFrame({"order_date": ["not-a-date", "2024-01-15"]})
        result = standardize_dates(df)
        assert len(result) == 1


class TestCleanPrices:
    """Tests for price cleaning."""

    def test_plain_number(self):
        df = pd.DataFrame({"price": ["100.00"]})
        result = clean_prices(df)
        assert result["price"].iloc[0] == 100.00

    def test_dollar_prefix(self):
        df = pd.DataFrame({"price": ["$50.00"]})
        result = clean_prices(df)
        assert result["price"].iloc[0] == 50.00

    def test_usd_prefix(self):
        df = pd.DataFrame({"price": ["USD 75.50"]})
        result = clean_prices(df)
        assert result["price"].iloc[0] == 75.50

    def test_comma_separator(self):
        df = pd.DataFrame({"price": ["1,234.56"]})
        result = clean_prices(df)
        assert result["price"].iloc[0] == 1234.56

    def test_integer_price(self):
        df = pd.DataFrame({"price": ["99"]})
        result = clean_prices(df)
        assert result["price"].iloc[0] == 99.0


class TestCleanCategories:
    """Tests for category standardization."""

    def test_lowercase(self):
        df = pd.DataFrame({"category": ["ELECTRONICS"]})
        result = clean_categories(df)
        assert result["category"].iloc[0] == "electronics"

    def test_typo_fix(self):
        df = pd.DataFrame({"category": ["Electronicss"]})
        result = clean_categories(df)
        assert result["category"].iloc[0] == "electronics"

    def test_variation(self):
        df = pd.DataFrame({"category": ["Home & Garden"]})
        result = clean_categories(df)
        assert result["category"].iloc[0] == "home"


class TestRemoveInvalidRows:
    """Tests for invalid row filtering."""

    def test_removes_negative_price(self):
        df = pd.DataFrame({
            "price": [10.0, -5.0, 20.0],
            "quantity": ["1", "2", "3"],
        })
        result = remove_invalid_rows(df)
        assert len(result) == 2

    def test_removes_zero_quantity(self):
        df = pd.DataFrame({
            "price": [10.0, 20.0],
            "quantity": ["0", "3"],
        })
        result = remove_invalid_rows(df)
        assert len(result) == 1


class TestCalculateTotalPrice:
    """Tests for total_price calculation."""

    def test_basic_calculation(self):
        df = pd.DataFrame({
            "price": [10.0, 25.50],
            "quantity": [2, 3],
        })
        result = calculate_total_price(df)
        assert result["total_price"].iloc[0] == 20.0
        assert result["total_price"].iloc[1] == 76.5
