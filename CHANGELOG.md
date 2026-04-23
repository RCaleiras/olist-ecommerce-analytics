# Changelog

All notable changes to this project are documented here.

---

## [0.3.0] — 2026-04-23

### Added
- Dimensions layer — 4 models: `dim_dates`, `dim_customers`, `dim_sellers`, `dim_products`
- Facts layer — 4 models: `fct_orders`, `fct_order_items`, `fct_payments`, `fct_reviews`
- 112 dbt tests passing across staging, dimensions and facts layers

### Fixed
- `dim_customers` deduplicated by `customer_unique_id` to resolve 2,997 duplicate rows
- `fct_orders` joined to `stg_customers` directly to resolve null `customer_unique_id` on multi-order customers

---

## [0.2.0] — 2026-04-23

### Added
- dbt project setup (`ecommerce_olist/`) with Postgres connection
- Raw data loading script (`exploration/load_raw_data.py`) — 9 tables into `raw` schema
- Staging layer — 8 models: `stg_orders`, `stg_order_items`, `stg_payments`,
  `stg_reviews`, `stg_customers`, `stg_sellers`, `stg_products`, `stg_geolocation`
- 64 dbt tests passing across staging layer

---

## [0.1.0] — 2026-04-21

### Added
- EDA script (`exploration/olist_eda.py`) covering data quality profiling, temporal coverage, referential integrity checks, and visualisations across all 9 source tables
- Modelling decisions log (`exploration/eda_notes.md`) with findings from EDA and confirmed architectural choices for the dbt layer
- Initial repository structure with `.gitignore` and `requirements.txt`