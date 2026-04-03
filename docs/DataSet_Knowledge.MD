# Olist Brazilian E-Commerce — Dataset Knowledge

Persistent reference for the **Brazilian E-Commerce Public Dataset by Olist** used in this repo. **Primary analytics target: Databricks** (Spark SQL, PySpark, Delta Lake). **Supabase / PostgreSQL** is a fallback for lightweight SQL only.

---

## 1. What it is

- Real, **anonymized** transactional data from Olist (Brazil), **~2016–2018**.
- **~99k orders** at the grain of `olist_orders_dataset`; other tables are items, payments, reviews, dimensions, and geo lookup.
- Review and partner references are masked (e.g. fictional names in text).
- **Official source:** [Kaggle — Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
- Optional add-on (not in this repo): Olist **Marketing Funnel** dataset for funnel stages.

---

## 2. Files in this repository

All CSVs live under:

`datastore/Brazilian_E_Commerce/`

| File | Approx. rows (excl. header) | Role |
|------|----------------------------|------|
| `olist_orders_dataset.csv` | 99,441 | **Spine:** order lifecycle, timestamps, status |
| `olist_order_items_dataset.csv` | 112,650 | Line items: product, seller, price, freight |
| `olist_order_payments_dataset.csv` | 103,886 | Payments: type, installments, value (order can have multiple rows) |
| `olist_order_reviews_dataset.csv` | 104,164 | Reviews: score, PT text, timestamps |
| `olist_customers_dataset.csv` | 99,441 | Customer location; `customer_unique_id` = person across orders |
| `olist_sellers_dataset.csv` | 3,095 | Seller location |
| `olist_products_dataset.csv` | 32,951 | Product attributes; category name in **Portuguese** |
| `olist_geolocation_dataset.csv` | 1,000,163 | Zip prefix → lat/lng (many rows per prefix — not 1:1) |
| `product_category_name_translation.csv` | 71 | PT → English category labels |

Narrative context and tool-fit notes: `datastore/Brazilian_E_Commerce/intro.md` (ignore the stray first line if present). Rich HTML briefing: `datastore/olist_dataset_briefing.html`.

---

## 3. Column schemas (from actual CSV headers)

### `olist_orders_dataset.csv`

`order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`

### `olist_order_items_dataset.csv`

`order_id`, `order_item_id`, `product_id`, `seller_id`, `shipping_limit_date`, `price`, `freight_value`

### `olist_order_payments_dataset.csv`

`order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value`

### `olist_order_reviews_dataset.csv`

`review_id`, `order_id`, `review_score`, `review_comment_title`, `review_comment_message`, `review_creation_date`, `review_answer_timestamp`

### `olist_customers_dataset.csv`

`customer_id`, `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state`

### `olist_sellers_dataset.csv`

`seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state`

### `olist_products_dataset.csv`

`product_id`, `product_category_name`, `product_name_lenght`, `product_description_lenght`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`  
*(Olist ships the typo `lenght` in column names.)*

### `olist_geolocation_dataset.csv`

`geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng`, `geolocation_city`, `geolocation_state`

### `product_category_name_translation.csv`

`product_category_name`, `product_category_name_english`

---

## 4. Join keys and grain

- **Anchor key:** `order_id` — links orders → items, payments, reviews.
- **Customers:** `orders.customer_id` → `customers.customer_id`. Use `customer_unique_id` for **distinct people** (one person can have multiple `customer_id` values).
- **Items:** `order_items` → `products` on `product_id`, → `sellers` on `seller_id`.
- **Payments:** one order → **one or more** payment rows (`payment_sequential`). Sum `payment_value` per order for total paid (watch double-counting when joining to item-level revenue).
- **Revenue from items:** typically `SUM(price)` or `SUM(price + freight_value)` at item grain; align definitions with product questions.
- **Geo:** join `customer_zip_code_prefix` or `seller_zip_code_prefix` to `geolocation_zip_code_prefix`. Geolocation is **many rows per prefix** — aggregate (e.g. average lat/lng) or pick a rule before joining.
- **English categories:** join `products.product_category_name` to `product_category_name_translation.product_category_name`.

---

## 5. Databricks usage (preferred)

- **Ingest:** Upload CSVs to cloud storage (e.g. ADLS, S3) or DBFS; register as **bronze** Delta or external tables with explicit schema (infer schema only for exploration).
- **Medallion pattern:** bronze (raw CSV) → silver (typed columns, cleaned timestamps, deduped geo if needed) → gold (order-level KPIs, seller scorecards, monthly trends).
- **SQL:** Spark SQL in notebooks or Databricks SQL warehouse; same join logic as ANSI SQL with Spark types (`timestamp`, `decimal`).
- **PySpark:** DataFrames for transforms, window functions (e.g. seller rank by month), optional UDFs for haversine distance using geo columns.
- **Partitioning (if materializing large Delta tables):** e.g. `order_purchase_timestamp` by year/month for orders or facts derived from orders; geolocation is large — consider separate optimized dimension.
- **Unity Catalog:** register tables for lineage and ACLs across the nine entities.

Static CSVs: no real CDC; streaming/Auto Loader exercises are **simulated** unless you append files on a schedule.

---

## 6. Supabase / PostgreSQL (last resort)

- Fine for ad hoc SQL and small dashboards; **geolocation ~1M rows** is manageable but heavier than other tables.
- Load with `COPY` or ETL; define PKs/FKs explicitly (`order_id`, `customer_id`, etc.) for consistent plans.
- Not the primary target for PySpark, Delta, or large-scale Spark tuning in this project.

---

## 7. Query and modeling tips

- Treat **`orders` as the spine**; build outward to items, payments, reviews.
- **Time series:** `order_purchase_timestamp` (and delivery fields — many nulls possible for non-delivered statuses).
- **Quality:** filter `order_status` when defining “completed” orders; validate null delivery dates before SLA metrics.
- **Language:** categories in PT; review text often **Portuguese**.
- **LLM / agent context:** supply this doc plus explicit **CREATE TABLE** or Delta schemas when generating SQL or PySpark to reduce join errors.

---

## 8. Limitations (for test data)

- Batch/static only; no built-in streaming or schema evolution.
- Moderate size — great for **correctness and joins**, not for cluster stress tests.
- No app-level auth/RBAC in the dataset itself.

---

## Reference — folders and files

Use **`@docs/DataSet_Knowledge.MD`** (or this path) in Cursor when you want the assistant to ground answers in the dataset and Databricks-first assumptions.

### Documentation (`docs/`)

| Path | Purpose |
|------|---------|
| `docs/DataSet_Knowledge.MD` | **This file** — schema, joins, row counts, Databricks / Supabase notes |
| `docs/Rank.MD` | Tool ordering / roadmap for what to build or test next |
| `docs/tools/Databricks_Conversational_SQL.md` | **LIVE** — NL→SQL on Databricks warehouse (`sql-agent`) |
| `docs/tools/Databricks_Unity_Browser.md` | **LIVE** — read-only Unity Catalog browser (`unity-catalog`) |
| `docs/tools/SQL_PySpark_Coding_Agent.md` | Roadmap stub; points to Conversational SQL doc |
| `docs/tools/Databricks_Intelligence_Agent.md` | **Roadmap** — full Databricks MCP agent (`databricks-agent`) |
| `docs/tools/Data_Architecture_Advisor.md` | Warehouse / medallion / modeling advice |
| `docs/tools/Multi_Agent_Pipeline_Designer.md` | Pipeline / ETL design |
| `docs/tools/Meeting_Intelligence_Agent.md` | Meeting / unstructured text workflows |
| `docs/tools/Gmail_Intelligence.md` | Gmail-oriented agent |
| `docs/tools/GitHub_Code_Miner.md` | Repo / code exploration agent |
| `docs/tools/AI_Scout.md` | Research / scouting agent |
| `docs/tools/ReAct_Research_Agent.md` | ReAct-style research agent |
| `docs/tools/Personal_AI_Orchestrator.md` | Multi-agent orchestration |

### Datastore — dataset and briefings (`datastore/`)

| Path | Purpose |
|------|---------|
| `datastore/Brazilian_E_Commerce/` | **All nine Olist CSVs** (source tables) |
| `datastore/Brazilian_E_Commerce/olist_orders_dataset.csv` | Order spine |
| `datastore/Brazilian_E_Commerce/olist_order_items_dataset.csv` | Line items |
| `datastore/Brazilian_E_Commerce/olist_order_payments_dataset.csv` | Payments |
| `datastore/Brazilian_E_Commerce/olist_order_reviews_dataset.csv` | Reviews |
| `datastore/Brazilian_E_Commerce/olist_customers_dataset.csv` | Customers |
| `datastore/Brazilian_E_Commerce/olist_sellers_dataset.csv` | Sellers |
| `datastore/Brazilian_E_Commerce/olist_products_dataset.csv` | Products |
| `datastore/Brazilian_E_Commerce/olist_geolocation_dataset.csv` | Geo lookup |
| `datastore/Brazilian_E_Commerce/product_category_name_translation.csv` | PT → EN categories |
| `datastore/Brazilian_E_Commerce/intro.md` | Narrative + tool-fit notes (ignore stray first line) |
| `datastore/olist_dataset_briefing.html` | Full HTML briefing + ERD-style diagram |
| `datastore/DataSet_silver.SQL` | Databricks draft: `CREATE OR REPLACE` CSV → `silver_olist_*` tables (replace `__CATALOG__` / `__SCHEMA__` / `__CSV_BASE__`) |
| `datastore/DataSet_gold.sql` | Gold star schema: `gold_dim_*` + `gold_fact_*` from silver (replace `__CATALOG__` / `__SILVER_SCHEMA__` / `__GOLD_SCHEMA__`) |
| `datastore/databricks_notebooks/DataSet_silver.py`, `DataSet_gold.py` | Databricks-importable notebooks (`# MAGIC %sql`); regenerate via `generate_from_sql.py` |

### Repo root (optional context)

| Path | Purpose |
|------|---------|
| `INFORMATION.txt` | High-level project information |

---

*Last aligned with on-disk CSVs under `datastore/Brazilian_E_Commerce/` (row counts from line counts including header row).*
