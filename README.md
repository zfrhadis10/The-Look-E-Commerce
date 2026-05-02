# 🏭 The Look E-Commerce — Data Warehouse & ETL Pipeline
### End-to-End ETL · Star Schema Data Modeling · PySpark · PostgreSQL

![PySpark](https://img.shields.io/badge/PySpark-Data%20Processing-E25A1C?style=flat&logo=apachespark&logoColor=white)
![BigQuery](https://img.shields.io/badge/Google%20BigQuery-Data%20Source-4285F4?style=flat&logo=googlebigquery&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-4169E1?style=flat&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Query%20%26%20DDL-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)

---

## 📌 Project Overview

**The Look** is a fictional e-commerce company whose transactional data lives across multiple scattered tables in **Google BigQuery**. Management needs fast, centralized, and reliable analytical reports — but raw OLTP data isn't built for that.

This project delivers a **production-style ETL pipeline** that:
1. **Extracts** 6 source tables from Google BigQuery (~1M+ total rows)
2. **Transforms** them using **PySpark** — cleaning, restructuring, and modeling into a Star Schema
3. **Loads** the resulting Fact + Dimension tables into a **PostgreSQL Data Warehouse**

> *"Turn scattered, unanalyzable transactional data into a clean, query-ready Data Warehouse — in one automated pipeline."*

---

## 🎯 Key Deliverables

| Deliverable | Detail |
|---|---|
| 📐 Data Model | **Star Schema** — 1 Fact Table + 5 Dimension Tables |
| 🔢 Source Data | **~1M+ rows** across 6 BigQuery tables |
| ⚙️ Processing Engine | **PySpark** (local Spark session with JDBC connector) |
| 🗄️ Destination | **PostgreSQL** Data Warehouse |
| 🧹 Cleaning Steps | Column pruning, null handling, null-string detection, type casting |
| 🔗 Fact Table Joins | **5-table multi-join** with aggregation to build `fact_sales` |
| 📊 Grain | 1 row = 1 item sold in 1 order (`order_item` level) |

---

## 🗂️ Data Architecture

### Star Schema Design

```
                    ┌──────────────────┐
                    │   dim_customer   │
                    │  customer_id (PK)│
                    │  first_name      │
                    │  last_name       │
                    │  email, age      │
                    │  gender          │
                    │  traffic_source  │
                    └────────┬─────────┘
                             │
┌──────────────────┐         │        ┌──────────────────┐
│   dim_date       │         │        │   dim_product    │
│  date_id (PK)    │         │        │  product_id (PK) │
│  order_date      │         ▼        │  name, category  │
│  day, month_num  ├──► fact_sales ◄──┤  brand, dept     │
│  month_name      │   sales_id (PK)  │  cost            │
│  quarter, year   │   order_id (FK)  │  retail_price    │
│  day_of_week     │   customer_id(FK)└──────────────────┘
│  is_weekend      │   product_id (FK)
└──────────────────┘   geography_id(FK)    ┌─────────────────────────┐
                        date_id (FK)       │  dim_distribution_center│
                        dist_center_id(FK) │  distribution_center_id │
                        quantity           │  name                   │
                        sales_amount  ◄────┤  latitude, longitude    │
                        cost_amount        └─────────────────────────┘
                             │
                    ┌────────┴─────────┐
                    │  dim_geography   │
                    │  geography_id(PK)│
                    │  city, state     │
                    │  country         │
                    └──────────────────┘
```

**Why Star Schema?** Simplified joins between fact and dimension tables maximize query performance and make it easy for analysts to slice data by customer, product, geography, date, or distribution center without complex nested queries.

---

## 🛠️ Tech Stack & Skills Demonstrated

| Category | Tools / Techniques |
|---|---|
| **Data Extraction** | `google-cloud-bigquery` — SQL queries against 6 BigQuery tables, exported to `.csv` |
| **Distributed Processing** | **PySpark** (`SparkSession`, `DataFrame API`) — scalable transformation engine |
| **PySpark Functions** | `col`, `regexp_replace`, `to_date`, `date_format`, `dayofmonth`, `month`, `quarter`, `year`, `dayofweek`, `monotonically_increasing_id`, `sum`, `count` |
| **Data Modeling** | **Star Schema** design — Fact Table + 5 Dimension Tables, grain definition |
| **Data Cleaning** | Column pruning, `dropna()`, `fillna()`, null-string detection with `rlike()` + `regexp_replace()`, duplicate checks |
| **Multi-table Joins** | 5-table PySpark join chain to construct `fact_sales` with FK resolution |
| **Date Dimension Engineering** | Extracted `day`, `month_num`, `month_name`, `quarter`, `year`, `day_of_week`, `is_weekend` from raw timestamp |
| **Database Integration** | **JDBC** connection — PySpark → PostgreSQL via `org.postgresql.Driver` |
| **DDL & SQL** | PostgreSQL table creation scripts with proper PK/FK constraints |
| **Data Warehousing** | OLTP → OLAP transformation, dimensional modeling concepts |

---

## 🔄 ETL Pipeline Walkthrough

### 1️⃣ EXTRACT — Google BigQuery → CSV → PySpark DataFrame

6 source tables extracted from the `bigquery-public-data.thelook_ecommerce` dataset:

| Table | Rows | Description |
|---|---|---|
| `users` | ~100K | Customer profiles |
| `products` | ~29K | Product catalog |
| `orders` | ~125K | Order history & status |
| `order_items` | ~250K | Item-level transaction details |
| `distribution_centers` | 10 | Warehouse locations |
| `inventory` | ~490K | Product-to-warehouse mapping |

Each table was exported as `.csv` and loaded into PySpark DataFrames via `spark.read.csv()`.

---

### 2️⃣ TRANSFORM — Data Cleaning + Dimensional Modeling

**Step 1 — Column Pruning** (remove irrelevant fields)

| Table | Dropped Columns | Reason |
|---|---|---|
| `users` | `street_address`, `postal_code`, `latitude`, `longitude`, `user_geom` | Geom format unusable in DW; city/state/country sufficient |
| `products` | `sku` | Internal operational code, not needed for sales analysis |
| `orders` | `gender`, `status`, `returned_at`, `shipped_at`, `delivered_at`, `num_of_item` | Gender duplicated in users; shipping/return out of scope |
| `order_items` | `inventory_item_id`, `shipped_at`, `delivered_at`, `returned_at` | Internal stock ID; logistics dates out of scope |
| `distribution_centers` | `distribution_center_geom` | Binary geometry format, lat/long sufficient |

**Step 2 — Missing Value Handling**

| Issue | Column | Rows Affected | Strategy |
|---|---|---|---|
| True nulls | `products.name` | 2 rows | `dropna()` — negligible impact |
| Unbranded products | `products.brand` | ~4K rows | `fillna("Unknown")` — preserve valid transactions |
| Null-string typos | `users.city` | Multiple | `regexp_replace()` — "nul", "nules" → "Unknown" |

> ⚠️ **Key insight**: Null-string typos (`"nul"`, `"nules"`) bypass standard `isNull()` checks — detected manually with `rlike("(?i).*nul.*")` pattern matching.

**Step 3 — Build 5 Dimension Tables**

Each dimension was constructed from cleaned source data with renamed PKs for FK clarity in the fact table:

```python
dim_customer       ← users_clean       (id → customer_id)
dim_product        ← products_clean    (id → product_id)
dim_geography      ← users_clean       (city/state/country + generated geography_id)
dim_date           ← orders_clean      (created_at → date components, date_id as YYYYMMDD int)
dim_distribution_center ← dist_clean   (id → distribution_center_id)
```

**Step 4 — Build Fact Table via 5-Table Multi-Join**

```python
fact_sales = order_items_clean
    .join(orders_clean,    on="order_id")          # → user_id, created_at
    .join(products_clean,  on="product_id")         # → product FK
    .join(users_clean,     on="user_id")            # → customer FK + city
    .join(inventory,       on="product_id")         # → distribution_center link
    .join(dim_geography,   on=["city", "country"])  # → geography FK
    .groupBy(FKs)
    .agg(
        count("item_id")     → quantity,
        sum("sale_price")    → sales_amount,
        cost × quantity      → cost_amount
    )
```

---

### 3️⃣ LOAD — PySpark JDBC → PostgreSQL

Tables loaded in dependency order to satisfy FK constraints:

```
1st wave (no dependencies):
  dim_customer | dim_product | dim_geography | dim_date | dim_distribution_center

2nd wave (requires all dimension tables):
  fact_sales
```

```python
df.write.jdbc(
    url="jdbc:postgresql://host.docker.internal:5432/GC6",
    table="table_name",
    mode="overwrite",
    properties={"user": ..., "driver": "org.postgresql.Driver"}
)
```

---

## 📁 Project Structure

```
📦 thelook-data-warehouse
 ┣ 📓 thelook_etl_pipeline.ipynb              # ETL notebook (PySpark)
 ┣ 📁 Dataset GC 6/
 ┃   ┣ 📄 users.csv
 ┃   ┣ 📄 products.csv
 ┃   ┣ 📄 orders.csv
 ┃   ┣ 📄 order_items.csv
 ┃   ┣ 📄 distribution_centers.csv
 ┃   ┗ 📄 inventory.csv
 ┣ 📊 thelook_etl_presentation.pdf            # Presentation slides
 ┗ 📄 README.md
```

---


## 🔗 References & Resources

| Resource | Link |
|---|---|
| 🗃️ Source Dataset | [BigQuery — thelook_ecommerce](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=thelook_ecommerce) |

---

---

## 👩‍💻 About

**Zafirah Aida Adista** — Data Analyst with data engineering chops, passionate about building pipelines that turn messy raw data into reliable analytical assets.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/your-profile)
[![GitHub](https://img.shields.io/badge/GitHub-Portfolio-181717?style=flat&logo=github&logoColor=white)](https://github.com/your-username)
[![Tableau](https://img.shields.io/badge/Tableau-Public%20Profile-E97627?style=flat&logo=tableau&logoColor=white)](https://public.tableau.com/app/profile/zafirah.adista)
