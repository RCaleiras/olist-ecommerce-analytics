"""
Olist E-Commerce — Exploração Inicial do Dataset
=================================================
Objectivo: avaliar qualidade dos dados, nulos, períodos temporais e
integridade referencial entre tabelas — antes de desenhar a arquitectura dbt.

Pré-requisitos:
  pip install pandas numpy matplotlib seaborn

Uso:
  1. Coloca todos os CSVs do Kaggle numa pasta (ex: ./data/olist/).
  2. Ajusta DATA_DIR abaixo.
  3. python olist_eda.py
     → Imprime relatório no terminal + grava olist_eda_report.txt
     → Grava olist_eda_plots.png com painéis visuais
"""

import os
import sys
import textwrap
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# CONFIG & PATHS
# ─────────────────────────────────────────────
DATA_DIR = Path(r"C:\Users\rdcal\Desktop\PORTFOLIO\project_2_ecommerce_analytics\data\olist")
REPORT_PATH = Path("olist_eda_report.txt")
PLOT_PATH   = Path("olist_eda_plots.png")

# Mapping: alias → CSV filenames
FILES = {
    "orders":       "olist_orders_dataset.csv",
    "items":        "olist_order_items_dataset.csv",
    "payments":     "olist_order_payments_dataset.csv",
    "reviews":      "olist_order_reviews_dataset.csv",
    "customers":    "olist_customers_dataset.csv",
    "sellers":      "olist_sellers_dataset.csv",
    "products":     "olist_products_dataset.csv",
    "geolocation":  "olist_geolocation_dataset.csv",
    "translation":  "product_category_name_translation.csv",
}

# Timestamp columns by table (for temporal analysis)
TIMESTAMP_COLS = {
    "orders":  ["order_purchase_timestamp", "order_approved_at",
                "order_delivered_carrier_date", "order_delivered_customer_date",
                "order_estimated_delivery_date"],
    "items":   ["shipping_limit_date"],
    "reviews": ["review_creation_date", "review_answer_timestamp"],
}

# ─────────────────────────────────────────────
# Output Utilities
# ─────────────────────────────────────────────
_report_lines = []

def rprint(*args, **kwargs):
    line = " ".join(str(a) for a in args)
    print(line, **kwargs)
    _report_lines.append(line)

def section(title: str):
    rprint()
    rprint("═" * 70)
    rprint(f"  {title}")
    rprint("═" * 70)

def subsection(title: str):
    rprint()
    rprint(f"── {title} " + "─" * (65 - len(title)))

# ─────────────────────────────────────────────
# 1. Loading CSV files into DataFrames
# ─────────────────────────────────────────────
section("1. FILE LOADING")

tables = {}
for alias, fname in FILES.items():
    fpath = DATA_DIR / fname
    if not fpath.exists():
        rprint(f"  [WARNING] File Not Found: {fpath}")
        continue
    df = pd.read_csv(fpath, low_memory=False)
    tables[alias] = df
    rprint(f"  {alias:<14}  {df.shape[0]:>8,} lines  ×  {df.shape[1]:>2} columns   ({fname})")

if not tables:
    rprint("\n[ERROR] No files found. Check DATA_DIR.")
    sys.exit(1)

# Parse de timestamps
for alias, cols in TIMESTAMP_COLS.items():
    if alias not in tables:
        continue
    for col in cols:
        if col in tables[alias].columns:
            tables[alias][col] = pd.to_datetime(tables[alias][col], errors='coerce')

# ─────────────────────────────────────────────
# 2. Perfil de qualidade — nulos e duplicados
# ─────────────────────────────────────────────
section("2. QUALITY PROFILE BY TABLE")

quality_summary = {}

for alias, df in tables.items():
    subsection(alias)
    n = len(df)

    # Nulos
    null_counts = df.isnull().sum()
    null_pct    = (null_counts / n * 100).round(2)
    cols_with_nulls = null_counts[null_counts > 0]

    if cols_with_nulls.empty:
        rprint("  Nulls: none ✓")
    else:
        rprint(f"  Nulls found in {len(cols_with_nulls)} column(s):")
        for col in cols_with_nulls.index:
            flag = "⚠ " if null_pct[col] > 5 else "  "
            rprint(f"    {flag}{col:<45} {null_counts[col]:>6,}  ({null_pct[col]:.1f}%)")

    # Duplicados totais (linha inteira)
    n_dupes = df.duplicated().sum()
    rprint(f"  Duplicated (complete lines): {n_dupes:,}  ({n_dupes/n*100:.2f}%)")

    quality_summary[alias] = {
        "rows": n,
        "cols": len(df.columns),
        "null_cols": len(cols_with_nulls),
        "full_dupes": n_dupes,
    }

# ─────────────────────────────────────────────
# 3. Temporal coverage and order delivery latencies
# ─────────────────────────────────────────────
section("3. TEMPORAL COVERAGE AND ORDER DELIVERY LATENCIES")

if "orders" in tables:
    orders = tables["orders"]
    ts_col = "order_purchase_timestamp"
    if ts_col in orders.columns:
        valid = orders[ts_col].dropna()
        rprint(f"\n  order_purchase_timestamp:")
        rprint(f"    Minimum : {valid.min()}")
        rprint(f"    Maximum : {valid.max()}")
        rprint(f"    Nulls  : {orders[ts_col].isnull().sum():,}")
        rprint(f"    Period: {(valid.max() - valid.min()).days} days")

        # Ordens por ano-mês
        orders["ym"] = orders[ts_col].dt.to_period("M")
        ym_counts = orders.groupby("ym").size()
        rprint(f"\n  Orders by Year-Month (first and last 6):")
        combined = pd.concat([ym_counts.head(6), ym_counts.tail(6)])
        for ym, cnt in combined.items():
            rprint(f"    {ym}  →  {cnt:,}")

        # Ordens por status
        if "order_status" in orders.columns:
            subsection("Order status distribution")
            status_dist = orders["order_status"].value_counts()
            total = len(orders)
            for s, c in status_dist.items():
                rprint(f"  {s:<25} {c:>7,}  ({c/total*100:.1f}%)")

    # Latências de entrega
    subsection("Delivery latencies (days)")
    needed = ["order_purchase_timestamp", "order_delivered_customer_date",
              "order_estimated_delivery_date"]
    if all(c in orders.columns for c in needed):
        o = orders.copy()
        o["days_to_deliver"] = (
            o["order_delivered_customer_date"] - o["order_purchase_timestamp"]
        ).dt.days
        o["days_vs_estimate"] = (
            o["order_delivered_customer_date"] - o["order_estimated_delivery_date"]
        ).dt.days

        delivered = o[o["order_status"] == "delivered"] if "order_status" in o.columns else o
        delivered = delivered.dropna(subset=["days_to_deliver"])

        rprint(f"  Orders delivered:  {len(delivered):,}")
        rprint(f"  Days to delivery  — median: {delivered['days_to_deliver'].median():.0f}  "
               f"p95: {delivered['days_to_deliver'].quantile(0.95):.0f}  "
               f"max: {delivered['days_to_deliver'].max():.0f}")
        late = delivered[delivered["days_vs_estimate"] > 0]
        rprint(f"  Late deliveries: {len(late):,}  ({len(late)/len(delivered)*100:.1f}%)")

# ─────────────────────────────────────────────
# 4. Referential integrity checks (foreign keys and primary keys)
# ─────────────────────────────────────────────
section("4. REFERENTIAL INTEGRITY")

def check_fk(child_alias, child_col, parent_alias, parent_col, label=None):
    if child_alias not in tables or parent_alias not in tables:
        return
    child  = tables[child_alias][child_col].dropna()
    parent = tables[parent_alias][parent_col].dropna()
    orphans = child[~child.isin(parent)].nunique()
    label = label or f"{child_alias}.{child_col} → {parent_alias}.{parent_col}"
    status = "✓" if orphans == 0 else f"⚠  {orphans:,} orphaned values"
    rprint(f"  {label:<55} {status}")

rprint()
check_fk("items",    "order_id",    "orders",    "order_id")
check_fk("payments", "order_id",    "orders",    "order_id")
check_fk("reviews",  "order_id",    "orders",    "order_id")
check_fk("orders",   "customer_id", "customers", "customer_id")
check_fk("items",    "product_id",  "products",  "product_id")
check_fk("items",    "seller_id",   "sellers",   "seller_id")
check_fk("products", "product_category_name", "translation", "product_category_name")

# Primary keys — uniqueness
subsection("Uniqueness of primary keys")

pk_checks = {
    "orders":    "order_id",
    "customers": "customer_id",
    "sellers":   "seller_id",
    "products":  "product_id",
    "reviews":   "review_id",
}
for alias, pk in pk_checks.items():
    if alias not in tables or pk not in tables[alias].columns:
        continue
    df    = tables[alias]
    dupes = df[pk].duplicated().sum()
    n     = len(df)
    status = "✓ unique" if dupes == 0 else f"⚠  {dupes:,} duplicates"
    rprint(f"  {alias}.{pk:<40} {n:>8,} rows   PK: {status}")

# items: PK composta (order_id + order_item_id)
# items: Composite PK (order_id + order_item_id)
if "items" in tables:
    df = tables["items"]
    if "order_id" in df.columns and "order_item_id" in df.columns:
        dupes = df.duplicated(subset=["order_id", "order_item_id"]).sum()
        status = "✓ unique" if dupes == 0 else f"⚠  {dupes:,} duplicates"
        rprint(f"  items.(order_id+order_item_id)             {len(df):>8,} rows   PK: {status}")

# ─────────────────────────────────────────────
# 5. Payments Analysis
# ─────────────────────────────────────────────
section("5. PAYMENTS")

if "payments" in tables:
    pay = tables["payments"]

    if "payment_type" in pay.columns:
        subsection("Types of payment")
        pt = pay["payment_type"].value_counts()
        for t, c in pt.items():
            rprint(f"  {t:<25} {c:>8,}  ({c/len(pay)*100:.1f}%)")

    if "payment_installments" in pay.columns:
        subsection("Payment Installments distribution")
        inst = pay["payment_installments"].value_counts().sort_index().head(12)
        for i, c in inst.items():
            rprint(f"  {i:>3}x  →  {c:>8,}")

    # Orders with multiple payments
    if "order_id" in pay.columns:
        multi = pay.groupby("order_id").size()
        rprint(f"\n  Orders with single payment  : {(multi==1).sum():,}")
        rprint(f"  Orders with 2+ payments    : {(multi>1).sum():,}")

    if "payment_value" in pay.columns:
        subsection("Distribution of payment_value (per row)")
        pv = pay["payment_value"]
        rprint(f"  Min: {pv.min():.2f}   Median: {pv.median():.2f}   "
               f"p95: {pv.quantile(0.95):.2f}   Max: {pv.max():.2f}")
        rprint(f"  Zeros/Negatives: {(pv <= 0).sum():,}")

# ─────────────────────────────────────────────
# 6. Reviews Analysis
# ─────────────────────────────────────────────
section("6. REVIEWS")

if "reviews" in tables:
    rev = tables["reviews"]

    if "review_score" in rev.columns:
        subsection("Distribution of review_score")
        rs = rev["review_score"].value_counts().sort_index()
        for score, cnt in rs.items():
            rprint(f"  Stars {score}:  {cnt:>7,}  ({cnt/len(rev)*100:.1f}%)")
        rprint(f"\n  Average Score: {rev['review_score'].mean():.3f}")

    if "review_id" in rev.columns:
        dupes_rev = rev["review_id"].duplicated().sum()
        rprint(f"  review_id duples: {dupes_rev:,}")

    # Review response time (days) — should be non-negative; negative values indicate data issues
    if all(c in rev.columns for c in ["review_creation_date","review_answer_timestamp"]):
        rev["response_days"] = (
            rev["review_answer_timestamp"] - rev["review_creation_date"]
        ).dt.days
        valid = rev["response_days"].dropna()
        rprint(f"\n  Response Time (days):")
        rprint(f"  Median: {valid.median():.0f}   p95: {valid.quantile(0.95):.0f}   "
               f"Negatives (anomaly): {(valid < 0).sum():,}")

# ─────────────────────────────────────────────
# 7. Products & Categories
# ─────────────────────────────────────────────
section("7. PRODUCTS & CATEGORIES")

if "products" in tables:
    prod = tables["products"]
    subsection("Category analysis")
    if "product_category_name" in prod.columns:
        n_cats    = prod["product_category_name"].nunique()
        n_nullcat = prod["product_category_name"].isnull().sum()
        rprint(f"  Unique Categories   : {n_cats}")
        rprint(f"  Products without Category   : {n_nullcat:,}  ({n_nullcat/len(prod)*100:.1f}%)")

    if "translation" in tables:
        trans = tables["translation"]
        pt_cats = set(prod["product_category_name"].dropna())
        en_cats = set(trans["product_category_name"].dropna())
        untranslated = pt_cats - en_cats
        rprint(f"  Categories without EN translation: {len(untranslated)}")
        if untranslated:
            for c in sorted(untranslated):
                rprint(f"    - {c}")

    # Physical dimensions — check for nulls and outliers
    dim_cols = ["product_weight_g","product_length_cm",
                "product_height_cm","product_width_cm"]
    dim_nulls = {c: prod[c].isnull().sum() for c in dim_cols if c in prod.columns}
    if dim_nulls:
        subsection("Nulls in Physical Dimensions")
        for col, n in dim_nulls.items():
            rprint(f"  {col:<30} {n:>5,} nulls  ({n/len(prod)*100:.1f}%)")

# ─────────────────────────────────────────────
# 8. Geolocation
# ─────────────────────────────────────────────
section("8. GEOLOCATION")

if "geolocation" in tables:
    geo = tables["geolocation"]
    rprint(f"\n  Total Records   : {len(geo):,}")
    if "geolocation_zip_code_prefix" in geo.columns:
        rprint(f"  Unique ZIP Codes         : {geo['geolocation_zip_code_prefix'].nunique():,}")
        # Duplicates by ZIP Code (multiple coordinates per ZIP Code — normal, requires aggregation)
        dup_cep = geo.duplicated(subset=["geolocation_zip_code_prefix"]).sum()
        rprint(f"  ZIP Codes with duplicate coords.: {dup_cep:,}  (expected — 1 ZIP Code has multiple coordinates)")
    # Coordenadas fora do Brasil
    if all(c in geo.columns for c in ["geolocation_lat","geolocation_lng"]):
        out_br = geo[
            (geo["geolocation_lat"] < -34) | (geo["geolocation_lat"] > 6) |
            (geo["geolocation_lng"] < -74) | (geo["geolocation_lng"] > -34)
        ]
        rprint(f"  Coords out of Brazil: {len(out_br):,}")

# ─────────────────────────────────────────────
# 9. Modeling Decisions for dbt Architecture
# ─────────────────────────────────────────────
section("9. NOTES FOR DBT ARCHITECTURE DESIGN")

notes = """
  Based on the above exploration, the following points should guide
  the design of the dbt models:

  STAGING (stg_):
  ├─ stg_orders        — cast of timestamps; rename verbose columns
  ├─ stg_order_items   — composite PK (order_id + order_item_id)
  ├─ stg_payments      — attention to orders with multiple payment methods
  ├─ stg_reviews       — review_id may have duplicates → dedup necessary
  ├─ stg_customers     — customer_unique_id ≠ customer_id (check cardinality)
  ├─ stg_sellers       — simple; just rename + typing
  ├─ stg_products      — join with translation for EN name; nulls in physical dimensions
  └─ stg_geolocation   — aggregate by zip_code_prefix (median lat/lng)

  MARTS — dimensions:
  ├─ dim_customers      — customer_unique_id as real PK (1 customer, N order_ids)
  ├─ dim_sellers        — geolocation (state, city) + performance metrics (from mart_seller_perf)
  ├─ dim_products       — categoria EN; dimensões físicas tratadas
  └─ dim_dates          — gerada em dbt (cobertura do período dos orders)

  MARTS — factos:
  ├─ fct_orders         — grão: 1 entry per order; value and latency metrics
  ├─ fct_order_items    — grão: 1 entry per item; price + freight_value
  ├─ fct_payments       — grão: 1 entry per payment (can have multiple per order)
  └─ fct_reviews        — grão: 1 entry per review (dedup in staging)

  MARTS — analytics:
  ├─ mart_rfm           — Recency/Frequency/Monetary per customer_unique_id
  ├─ mart_cohorts       — monthly retention (1st purchase as cohort)
  └─ mart_seller_perf   — performance and rating per seller

  QUALITY ALERTS TO COVER WITH dbt tests:
  • orders.order_id     — not_null, unique
  • items.order_id      — not_null, relationships(orders)
  • payments.order_id   — not_null, relationships(orders)
  • reviews.order_id    — not_null, relationships(orders)
  • orders delivered without order_delivered_customer_date — accepted_values / custom
  • payment_value >= 0  — custom test
"""
rprint(notes)

# ─────────────────────────────────────────────
# 10. Visualizations
# ─────────────────────────────────────────────
section("10. GENERATING VISUALIZATIONS → " + str(PLOT_PATH))

try:
    sns.set_theme(style="whitegrid", palette="muted", font_scale=0.9)
    fig = plt.figure(figsize=(20, 18))
    fig.suptitle("Olist E-Commerce — EDA Overview", fontsize=16, fontweight="bold", y=0.98)
    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35)

    # ── A. Order by month ──────────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, :2])
    if "orders" in tables and "ym" in tables["orders"].columns:
        ym_s = tables["orders"].groupby("ym").size()
        ym_s.index = ym_s.index.astype(str)
        ax1.bar(ym_s.index, ym_s.values, color="#4C72B0", width=0.8)
        ax1.set_title("Orders by Month", fontweight="bold")
        ax1.set_xlabel("Year-Month"); ax1.set_ylabel("No. of Orders")
        ax1.tick_params(axis='x', rotation=45)

    # ── B. Order_status distribution ───────────────────────────────
    ax2 = fig.add_subplot(gs[0, 2])
    if "orders" in tables and "order_status" in tables["orders"].columns:
        sc = tables["orders"]["order_status"].value_counts()
        ax2.barh(sc.index, sc.values, color="#DD8452")
        ax2.set_title("Order Status Distribution", fontweight="bold")
        ax2.set_xlabel("No. of Orders")

    # ── C. Review scores ──────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[1, 0])
    if "reviews" in tables and "review_score" in tables["reviews"].columns:
        rs = tables["reviews"]["review_score"].value_counts().sort_index()
        ax3.bar(rs.index.astype(str), rs.values,
                color=["#d62728","#ff7f0e","#ffbb78","#98df8a","#2ca02c"])
        ax3.set_title("Review Score Distribution", fontweight="bold")
        ax3.set_xlabel("Stars"); ax3.set_ylabel("No. of Reviews")

    # ── D. Payment types ─────────────────────────────────────────
    ax4 = fig.add_subplot(gs[1, 1])
    if "payments" in tables and "payment_type" in tables["payments"].columns:
        pt = tables["payments"]["payment_type"].value_counts()
    
        wedges, texts, autotexts = ax4.pie(
            pt.values,
            labels=None,                   
            autopct="%1.1f%%",
            rotatelabels=True,
            labeldistance=1.05,
            pctdistance=1.1,               
            startangle=90,
            colors=sns.color_palette("muted", len(pt)),
            wedgeprops={"edgecolor": "white", "linewidth": 1.5}
        )
    
        for t in autotexts:
            t.set_fontsize(9)
            t.set_fontweight("bold")
    
        ax4.legend(
            wedges,
            pt.index,
            title="Payment Type",
            loc="center left",
            bbox_to_anchor=(0.9, 0.5),      
            fontsize=9,
            frameon=False
        )
    
    ax4.set_title("Payment Types", fontweight="bold", pad=12)

    # ── E. Payment value distribution ─────────────────────────────
    ax5 = fig.add_subplot(gs[1, 2])
    if "payments" in tables and "payment_value" in tables["payments"].columns:
        pv = tables["payments"]["payment_value"]
        pv_clipped = pv[pv.between(0, pv.quantile(0.99))]
        ax5.hist(pv_clipped, bins=50, color="#8172B3", edgecolor="white")
        ax5.set_title("Payment Value (without top 1%)", fontweight="bold")
        ax5.set_xlabel("Value (R$)"); ax5.set_ylabel("Frequency")

    # ── F. Days to delivery (histogram) ──────────────────────────────
    ax6 = fig.add_subplot(gs[2, 0])
    if "orders" in tables:
        o = tables["orders"]
        if all(c in o.columns for c in ["order_purchase_timestamp",
                                         "order_delivered_customer_date"]):
            dtd = (o["order_delivered_customer_date"] -
                   o["order_purchase_timestamp"]).dt.days
            dtd_valid = dtd.dropna()
            dtd_clipped = dtd_valid[dtd_valid.between(0, 90)]
            ax6.hist(dtd_clipped, bins=45, color="#55A868", edgecolor="white")
            ax6.axvline(dtd_valid.median(), color="red", linestyle="--",
                        label=f"Median {dtd_valid.median():.0f}d")
            ax6.set_title("Days to Delivery (0–90d)", fontweight="bold")
            ax6.set_xlabel("Days"); ax6.set_ylabel("Frequency")
            ax6.legend(fontsize=8)

    # ── G. Nulls by Table (heatmap simplificado) ────────────────────
    ax7 = fig.add_subplot(gs[2, 1])
    null_data = {}
    for alias, df in tables.items():
        if alias == "geolocation": continue
        pct = (df.isnull().mean() * 100).round(1)
        null_data[alias] = pct[pct > 0] if (pct > 0).any() else pd.Series(dtype=float)
    all_null_cols = sorted(set(c for s in null_data.values() for c in s.index))
    if all_null_cols:
        null_matrix = pd.DataFrame(
            {alias: null_data[alias].reindex(all_null_cols, fill_value=0)
             for alias in null_data}
        ).T
        sns.heatmap(null_matrix, ax=ax7, cmap="YlOrRd", annot=True, fmt=".1f",
                    cbar_kws={"label": "% Nulls"}, linewidths=0.5, annot_kws={"size": 7})
        ax7.set_title("% Nulls by Table × Column", fontweight="bold")
        ax7.tick_params(axis='x', rotation=45, labelsize=7)
    else:
        ax7.text(0.5, 0.5, "Sem nulos relevantes ✓",
                 ha="center", va="center", transform=ax7.transAxes)
        ax7.set_title("% Nulls by Table × Column", fontweight="bold")

    # ── H. Top 10 categories by nº of products ───────────────────────
    ax8 = fig.add_subplot(gs[2, 2])
    if "products" in tables and "translation" in tables:
        prod = tables["products"].copy()
        trans = tables["translation"].copy()
        prod = prod.merge(trans, on="product_category_name", how="left")
        cat_col = "product_category_name_english" if "product_category_name_english" in prod.columns \
                   else "product_category_name"
        top10 = prod[cat_col].value_counts().head(10)
        ax8.barh(top10.index[::-1], top10.values[::-1], color="#C44E52")
        ax8.set_title("Top 10 Categories (nº products)", fontweight="bold")
        ax8.set_xlabel("Nº products")
        ax8.tick_params(axis='y', labelsize=8)

    plt.savefig(PLOT_PATH, dpi=130, bbox_inches="tight")
    rprint(f"  Plots saved to: {PLOT_PATH}")
    plt.close()

except Exception as e:
    rprint(f"  [WARNING] Error generating plots: {e}")

# ─────────────────────────────────────────────
# 11. Saving report to text file
# ─────────────────────────────────────────────
with open(REPORT_PATH, "w", encoding="utf-8") as f:
    f.write(f"Olist EDA Report — generated on {datetime.now():%Y-%m-%d %H:%M}\n")
    f.write("\n".join(_report_lines))

print(f"\n✓ Report saved to: {REPORT_PATH}")
print(f"✓ Script completed.\n")
