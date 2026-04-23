# Olist E-Commerce Analytics

> **"Is Olist's business model sustainable? Do sellers deliver with enough quality to retain customers — and do customers come back?"**

End-to-end analytics pipeline built on the public Olist Brazilian E-Commerce dataset, developed as a senior Data Analytics portfolio project.

---

## Tech Stack

| Layer | Tools |
|---|---|
| Data modelling & transformation | dbt Core, PostgreSQL |
| Visualisation & storytelling | Tableau Public |
| Exploration & scripting | Python (pandas, numpy, matplotlib, seaborn) |
| Version control | Git, GitHub |

---

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— ~100k orders, 9 CSV tables, covering the period 2016–2018.

> **Note:** Data is not included in this repository. Download the CSVs directly from Kaggle and place them under `data/olist/`.

---

## Business Themes

This project is structured around four analytical themes, each answering a slice of the central business question:

1. **Growth & Business Health** — Is the business growing sustainably? GMV trends, order volume, AOV, and order funnel analysis.
2. **Customer Behaviour & Value** — Who are the most valuable customers and can we retain them? RFM segmentation, cohort retention, LTV estimation.
3. **Operational Excellence, Satisfaction & Freight** — Is delivery performance affecting customer satisfaction? Delivery latency, review scores, freight cost analysis by region.
4. **Seller & Category Performance** — Which sellers and categories sustain — or threaten — the business? Pareto analysis, seller risk flags, category GMV and ratings.

---

## Architecture

```
raw (source CSVs loaded into Postgres)
  └─ staging (stg_*)       ← cleaning, casting, renaming, deduplication
       └─ marts
            ├─ dimensions (dim_*)     ← stable entities
            ├─ facts (fct_*)          ← transactional grain
            └─ analytics (mart_*)     ← aggregated business metrics
```

---

## Repository Structure

```
olist-ecommerce-analytics/
├── README.md
├── CHANGELOG.md
├── LICENSE
├── requirements.txt
├── exploration/
│   ├── olist_eda.py          ← EDA script
│   └── eda_notes.md          ← findings and modelling decisions
├── data/                     ← gitignored — place Kaggle CSVs here
│   └── olist/
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   │       ├── dimensions/
│   │       ├── facts/
│   │       └── analytics/
│   ├── tests/
│   ├── macros/
│   └── seeds/
└── tableau/
    └── screenshots/
```

---

## Dataset Limitations

These are known characteristics of the dataset, not analytical shortcomings — documented here for full transparency:

- **Time period:** September 2016 to August 2018. Data from 2016 is sparse (platform launch). Trend analyses start from January 2017.
- **Repeat purchase rate:** The majority of customers made a single purchase. Retention and LTV analyses are framed within this context — it is a business finding, not a data quality issue.
- **Geolocation:** Multiple coordinates per ZIP code prefix — aggregated to median lat/lng per prefix.
- **Reviews:** `review_id` contains 814 duplicates in the public dataset — deduplicated in staging.

---

## Key Findings

*(To be completed after dashboard is published)*

---

## How to Run

*(To be completed after dbt setup)*

---

## Dashboard

*(Tableau Public link to be added upon publication)*

---

## Project Status

| Stage | Status |
|---|---|
| 1 — EDA & exploration | ✅ Complete |
| 2 — dbt architecture | ✅ Complete |
| 3 — Staging models | ✅ Complete |
| 4 — Dimension & fact models | ✅ Complete |
| 5 — Analytics marts | 🔄 In progress |
| 6 — Tableau dashboard | ⬜ Pending |
| 7 — Documentation & publish | ⬜ Pending |