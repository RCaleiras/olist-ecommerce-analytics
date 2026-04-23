"""
Load Olist CSVs into PostgreSQL as raw source tables.
Run once before dbt pipeline.
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────────────
DATA_DIR    = Path(r"C:\Users\rdcal\Desktop\PORTFOLIO\project_2_ecommerce_analytics\data\olist")
DB_URL      = "postgresql://postgres:danieL#252515@localhost:5432/olist"
SCHEMA      = "raw"

FILES = {
    "orders":      "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments":    "olist_order_payments_dataset.csv",
    "reviews":     "olist_order_reviews_dataset.csv",
    "customers":   "olist_customers_dataset.csv",
    "sellers":     "olist_sellers_dataset.csv",
    "products":    "olist_products_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "translation": "product_category_name_translation.csv",
}

# ── Load ──────────────────────────────────────────────────────────────
engine = create_engine(DB_URL)

with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
    conn.commit()

for table, fname in FILES.items():
    fpath = DATA_DIR / fname
    if not fpath.exists():
        print(f"  [SKIP] {fname} not found")
        continue
    print(f"  Loading {table}...", end=" ")
    df = pd.read_csv(fpath, low_memory=False)
    df.to_sql(
        name=table,
        con=engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False
    )
    print(f"{len(df):,} rows ✓")

print("\nAll tables loaded into schema 'raw'.")