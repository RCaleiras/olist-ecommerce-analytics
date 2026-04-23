# Olist EDA — Findings & Modelling Decisions

Generated: 2026-04-21
Script: olist_eda.py

---

## 1. Temporal Coverage

- Raw period: 2016-09-04 to 2018-10-17 (772 days)
- 2016 data: residual (4 orders in September, 324 in October, 1 in December) — platform launch period
- 2018-09 and 2018-10: spurious (16 and 4 orders respectively) — dataset likely extracted mid-month
- **Effective analysis period: 2017-01 to 2018-08**
- Impact: `dim_dates` covers 2017-01-01 to 2018-08-31; trend and cohort analyses exclude data outside this range

---

## 2. Data Quality — Nulls

| Table    | Column                         | Nulls  | %     | Decision                                          |
|----------|--------------------------------|--------|-------|---------------------------------------------------|
| orders   | order_approved_at              | 160    | 0.2%  | Keep null — unapproved orders                     |
| orders   | order_delivered_carrier_date   | 1,783  | 1.8%  | Keep null — orders not yet dispatched             |
| orders   | order_delivered_customer_date  | 2,965  | 3.0%  | Keep null — orders not yet delivered              |
| reviews  | review_comment_title           | 87,656 | 88.3% | Column excluded from stg_reviews                  |
| reviews  | review_comment_message         | 58,247 | 58.7% | Column excluded from stg_reviews                  |
| products | product_category_name          | 610    | 1.9%  | COALESCE to 'unknown' in stg_products             |
| products | product_name_lenght            | 610    | 1.9%  | Excluded — not used in marts                      |
| products | product_description_lenght     | 610    | 1.9%  | Excluded — not used in marts                      |
| products | product_photos_qty             | 610    | 1.9%  | Excluded — not used in marts                      |
| products | product_weight_g               | 2      | 0.0%  | COALESCE to 0 in stg_products                     |
| products | product_length_cm              | 2      | 0.0%  | COALESCE to 0 in stg_products                     |
| products | product_height_cm              | 2      | 0.0%  | COALESCE to 0 in stg_products                     |
| products | product_width_cm               | 2      | 0.0%  | COALESCE to 0 in stg_products                     |

---

## 3. Referential Integrity

| FK Relationship                                   | Result                  | Decision                              |
|---------------------------------------------------|-------------------------|---------------------------------------|
| items.order_id → orders.order_id                  | ✓ No orphans            | Add relationships test in dbt         |
| payments.order_id → orders.order_id               | ✓ No orphans            | Add relationships test in dbt         |
| reviews.order_id → orders.order_id                | ✓ No orphans            | Add relationships test in dbt         |
| orders.customer_id → customers.customer_id        | ✓ No orphans            | Add relationships test in dbt         |
| items.product_id → products.product_id            | ✓ No orphans            | Add relationships test in dbt         |
| items.seller_id → sellers.seller_id               | ✓ No orphans            | Add relationships test in dbt         |
| products.category → translation.category          | ⚠ 2 orphans             | Handle manually — see below           |

**Categories missing English translation (orphans):**
- `pc_gamer` → manual translation: `"PC Gamer"`
- `portateis_cozinha_e_preparadores_de_alimentos` → manual translation: `"Portable Kitchen & Food Processors"`
- Strategy: add to the translation CSV seed in dbt, or handle with CASE WHEN in stg_products

---

## 4. Anomalies

| Table       | Anomaly                                   | Scale      | Decision                                        |
|-------------|-------------------------------------------|------------|-------------------------------------------------|
| reviews     | Duplicate review_id                       | 814 dupes  | Dedup in stg_reviews — keep most recent entry   |
| payments    | payment_value = 0                         | 9 rows     | Keep — likely 100% voucher payments             |
| payments    | payment_installments = 0                  | 2 rows     | Likely linked to not_defined payment type       |
| geolocation | Full duplicate rows                       | 261,831    | Deduplicate in stg_geolocation                  |
| geolocation | Coordinates outside Brazil                | 42 rows    | Filter out in stg_geolocation                   |

---

## 5. Confirmed Modelling Decisions

**Customer identifiers:**
- `customer_id` is order-scoped — one customer can have multiple `customer_id` values
- `customer_unique_id` is the true customer identifier
- `dim_customers` uses `customer_unique_id` as its primary key

**Geolocation:**
- Strategy: compute median lat/lng per `zip_code_prefix` after deduplication and coordinate filtering
- Brazil bounding box filter: lat between -34 and 6, lng between -74 and -34

**dim_dates coverage:**
- Range: 2017-01-01 to 2018-08-31
- Fields: date, year, month, quarter, day_of_week, is_weekend, is_weekday

**Order funnel:**
- 97.0% delivered, 1.1% shipped, 0.6% cancelled, 0.6% unavailable
- States with very low volume (created = 5, approved = 2) — include in funnel but annotate in dashboard

**Freight:**
- `freight_value` available in order_items — freight ratio angle is viable
- `freight_ratio = freight_value / price` — requires `safe_divide` macro to avoid division by zero

---

## 6. Fields Excluded from the Model

These fields exist in the raw source but will not be carried into dbt models:

| Field                      | Table    | Reason                                          |
|----------------------------|----------|-------------------------------------------------|
| review_comment_title       | reviews  | 88% null — no analytical value                  |
| review_comment_message     | reviews  | 58% null — text analysis out of scope           |
| product_name_lenght        | products | Typo in original source name; not used          |
| product_description_lenght | products | Not used in any business theme                  |
| product_photos_qty         | products | Not used in any business theme                  |