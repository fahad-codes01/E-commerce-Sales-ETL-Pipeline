"""
generate_data.py — Generate a dirty e-commerce orders dataset.

Creates ~1,000 rows of realistic order data with intentional data quality
issues for demonstrating ETL cleaning capabilities.

Dirty data scenarios:
  1. Missing values (~5% of rows)
  2. Inconsistent date formats (YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY)
  3. Inconsistent category naming (mixed case, typos)
  4. Invalid values (negative price or quantity)
  5. Mixed price formats ($100, USD 50, plain numbers)
  6. Duplicate rows (~3%)

Usage:
    python scripts/generate_data.py
"""

import csv
import os
import random
from datetime import datetime, timedelta

# ---- Configuration ----

NUM_ROWS = 1000
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "orders.csv")

PRODUCTS = {
    "electronics": [
        "Wireless Mouse", "Mechanical Keyboard", "USB-C Hub",
        "Webcam HD", "Bluetooth Speaker", "Laptop Stand",
        "Monitor 27 inch", "Portable SSD 1TB",
    ],
    "clothing": [
        "Cotton T-Shirt", "Denim Jeans", "Running Shoes",
        "Winter Jacket", "Baseball Cap", "Wool Socks",
    ],
    "books": [
        "Python Crash Course", "Data Engineering Handbook",
        "Clean Code", "SQL Cookbook", "Designing Data Apps",
    ],
    "home": [
        "Desk Lamp", "Coffee Mug", "Throw Pillow",
        "Wall Clock", "Scented Candle",
    ],
    "sports": [
        "Yoga Mat", "Resistance Bands", "Water Bottle",
        "Jump Rope", "Dumbbell Set",
    ],
}

PRICE_RANGES = {
    "electronics": (15.0, 500.0),
    "clothing": (10.0, 150.0),
    "books": (8.0, 45.0),
    "home": (5.0, 80.0),
    "sports": (10.0, 120.0),
}

# Category name variations for dirty data
CATEGORY_VARIATIONS = {
    "electronics": ["Electronics", "electronics", "ELECTRONICS", "Electronicss", "electronic"],
    "clothing": ["Clothing", "clothing", "CLOTHING", "Clothng", "clothes"],
    "books": ["Books", "books", "BOOKS", "Book", "book"],
    "home": ["Home", "home", "HOME", "Home & Garden", "home_goods"],
    "sports": ["Sports", "sports", "SPORTS", "Sport", "sport"],
}

# Date formats for dirty data
DATE_FORMATS = [
    "%Y-%m-%d",      # 2024-01-15   (standard)
    "%d/%m/%Y",      # 15/01/2024
    "%d-%m-%Y",      # 15-01-2024
    "%m/%d/%Y",      # 01/15/2024
    "%b %d, %Y",     # Jan 15, 2024
]


def random_date(start_date, end_date):
    """Generate a random date between start and end."""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)


def format_date_dirty(date_obj):
    """Format date using a randomly chosen format (dirty data)."""
    fmt = random.choice(DATE_FORMATS)
    return date_obj.strftime(fmt)


def format_price_dirty(price):
    """Format price with mixed formats (dirty data)."""
    roll = random.random()
    if roll < 0.60:
        # Normal number
        return f"{price:.2f}"
    elif roll < 0.75:
        # Dollar sign prefix
        return f"${price:.2f}"
    elif roll < 0.85:
        # USD prefix
        return f"USD {price:.2f}"
    elif roll < 0.95:
        # With comma separator
        return f"{price:,.2f}"
    else:
        # Integer (no decimals)
        return str(int(price))


def generate_dirty_dataset():
    """Generate the dirty orders dataset."""

    random.seed(42)

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    rows = []

    for i in range(1, NUM_ROWS + 1):
        # Pick a random category and product
        category = random.choice(list(PRODUCTS.keys()))
        product = random.choice(PRODUCTS[category])

        # Generate base values
        order_id = f"ORD-{i:04d}"
        order_date = format_date_dirty(random_date(start_date, end_date))
        customer_id = f"CUST-{random.randint(1, 200):04d}"
        price_min, price_max = PRICE_RANGES[category]
        price = float(f"{random.uniform(price_min, price_max):.2f}")
        quantity = random.randint(1, 5)

        # Use dirty category name
        dirty_category = random.choice(CATEGORY_VARIATIONS[category])

        # Format price with mixed formats
        dirty_price = format_price_dirty(price)

        row = [order_id, order_date, customer_id, product, dirty_category, dirty_price, quantity]
        rows.append(row)

    # --- Inject dirty data scenarios ---

    # 1. Missing values (~5% of rows → ~50 rows)
    num_missing = int(NUM_ROWS * 0.05)
    for _ in range(num_missing):
        idx = random.randint(0, len(rows) - 1)
        col = random.choice([2, 3, 4, 5])  # customer_id, product, category, price
        rows[idx][col] = ""

    # 2. Invalid values — negative price (~2%)
    num_neg_price = int(NUM_ROWS * 0.02)
    for _ in range(num_neg_price):
        idx = random.randint(0, len(rows) - 1)
        rows[idx][5] = f"-{random.uniform(10, 100):.2f}"

    # 3. Invalid values — negative/zero quantity (~2%)
    num_neg_qty = int(NUM_ROWS * 0.02)
    for _ in range(num_neg_qty):
        idx = random.randint(0, len(rows) - 1)
        rows[idx][6] = random.choice([-1, -2, 0, -3])

    # 4. Duplicate rows (~3%)
    num_duplicates = int(NUM_ROWS * 0.03)
    for _ in range(num_duplicates):
        idx = random.randint(0, len(rows) - 1)
        rows.append(rows[idx].copy())

    # Shuffle to mix duplicates in
    random.shuffle(rows)

    # Write to CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "order_date", "customer_id", "product", "category", "price", "quantity"])
        writer.writerows(rows)

    print(f"✅ Generated {len(rows)} rows (including dirty data) → {OUTPUT_FILE}")
    print(f"   - ~{num_missing} rows with missing values")
    print(f"   - ~{num_neg_price} rows with negative prices")
    print(f"   - ~{num_neg_qty} rows with negative/zero quantities")
    print(f"   - ~{num_duplicates} duplicate rows")
    print(f"   - Mixed date formats: {len(DATE_FORMATS)} variants")
    print(f"   - Mixed price formats: $, USD, comma, plain")
    print(f"   - Inconsistent categories: typos + mixed case")


if __name__ == "__main__":
    generate_dirty_dataset()
