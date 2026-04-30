# Data Warehouse Architecture - Kimball Star Schema với dbt

## 📊 Overview

Dự án này migrate data warehouse từ MySQL sang dbt với kiến trúc **Kimball Star Schema** gồm 3 layers — giữ nguyên thiết kế Bronze / Silver / Gold từ MySQL gốc:

```
┌─────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                         │
│              (Raw Data - tồn tại sẵn trong MySQL)           │
├─────────────────────────────────────────────────────────────┤
│  CRM System          │  ERP System                          │
│  • crm_cust_info     │  • erp_cust_az12 (attributes)       │
│  • crm_prd_info      │  • erp_loc_a101 (location)          │
│  • crm_sales_details │  • erp_px_cat_g1v2 (category)       │
│                      │                                      │
│  ← dbt KHÔNG tạo ra bronze, chỉ đọc qua source()  →        │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                       SILVER LAYER                          │
│              (Cleaned & Enriched - dbt intermediate/)       │
├─────────────────────────────────────────────────────────────┤
│  Clean Models (int_*) — 1-1 với bronze sources              │
│  • int_crm_cust_info      → clean customers                 │
│  • int_crm_prd_info       → clean products                  │
│  • int_sales              → clean sales                     │
│  • int_erp_cust_az12      → clean ERP customer attrs        │
│  • int_erp_loc_a101       → clean location                  │
│  • int_erp_px_cat_g1v2    → clean categories               │
│                                                              │
│  Enrich Models (int_*) — join các clean models lại          │
│  • int_customers_enriched → CRM + ERP attrs + Location      │
│  • int_products_enriched  → Products + Categories           │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                           │
│                  (Star Schema - Kimball)                    │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐         ┌──────────────────┐         │
│  │  dim_customers   │         │  dim_products    │         │
│  ├──────────────────┤         ├──────────────────┤         │
│  │ customer_key (PK)│         │ product_key (PK) │         │
│  │ customer_id (BK) │         │ product_id (BK)  │         │
│  │ first_name       │         │ product_name     │         │
│  │ last_name        │         │ category         │         │
│  │ marital_status   │         │ subcat           │         │
│  │ gender           │         │ maintenance      │         │
│  │ birthdate        │         │ valid_from       │         │
│  │ country          │         │ valid_to         │         │
│  │ valid_from       │         │ is_current       │         │
│  │ valid_to         │         └──────────────────┘         │
│  │ is_current       │                  │                    │
│  └──────────────────┘                  │                    │
│           │                            │                    │
│           │      ┌─────────────────────────────┐           │
│           └─────→│        fact_sales           │←──────────┘
│                  ├─────────────────────────────┤           │
│                  │ sales_key (PK)              │           │
│                  │ customer_key (FK)           │           │
│                  │ product_key (FK)            │           │
│                  │ order_number                │           │
│                  │ order_date                  │           │
│                  │ quantity                    │           │
│                  │ amount                      │           │
│                  │ total_amount                │           │
│                  └─────────────────────────────┘           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## 🎯 Key Features

### 1. **Slowly Changing Dimension Type 2 (SCD2)**

Cả `dim_customers` và `dim_products` implement SCD2 để track lịch sử thay đổi:

```
customer_id | first_name | country | valid_from  | valid_to    | is_current
------------|------------|---------|-------------|-------------|------------
123         | Hieu       | USA     | 2024-01-01  | 2024-06-15  | false
123         | Hieu       | CANADA  | 2024-06-15  | 9999-12-31  | true
```

### 2. **Point-in-Time Correct Joins**

Fact table join với dimension dựa trên `order_date` để lấy đúng version tại thời điểm bán hàng:

```sql
LEFT JOIN dim_customers c
    ON f.customer_id = c.customer_id
    AND f.order_date >= c.valid_from
    AND f.order_date < c.valid_to
```

### 3. **Incremental Loading**

Mỗi clean model dùng watermark riêng, giống MySQL `ctrl_watermark`:

| Model | Watermark column |
|---|---|
| `int_crm_cust_info` | `cst_create_date` (business date) |
| `int_crm_prd_info` | `prd_start_dt` (business date) |
| `int_sales` | `sls_order_dt` (business date) |
| `int_erp_cust_az12` | `ingestion_ts` (technical ts) |
| `int_erp_loc_a101` | `ingestion_ts` (technical ts) |
| `int_erp_px_cat_g1v2` | `ingestion_ts` (technical ts) |

### 4. **Data Quality Tests**

Automatic tests tương đương `ctrl_dq_log` trong MySQL:
- ✅ Primary key uniqueness
- ✅ Not null constraints
- ✅ Referential integrity (FK relationships)
- ✅ Custom business rules

## 📁 File Structure

```
dw_dbt/
│
├── dbt_project.yml          → Project config, materialization strategy
├── packages.yml             → dbt packages (dbt_utils, dbt_expectations)
├── profiles.yml             → MySQL connection config
│
├── models/
│   ├── sources/
│   │   └── _sources.yml     → Khai báo bronze_dw tables (không tạo, chỉ đọc)
│   │
│   ├── intermediate/        → Silver layer (clean + enrich)
│   │   ├── _intermediate.yml
│   │   │
│   │   │   -- Clean models (1-1 với bronze, incremental)
│   │   ├── int_crm_cust_info.sql        → Clean + dedup customers
│   │   ├── int_crm_prd_info.sql         → Clean + dedup products
│   │   ├── int_sales.sql                → Clean + dedup sales
│   │   ├── int_erp_cust_az12.sql        → Clean ERP customer attrs
│   │   ├── int_erp_loc_a101.sql         → Clean location
│   │   ├── int_erp_px_cat_g1v2.sql      → Clean categories
│   │   │
│   │   │   -- Enrich models (join clean models)
│   │   ├── int_customers_enriched.sql   → CRM + ERP + Location
│   │   └── int_products_enriched.sql    → Products + Categories
│   │
│   └── gold/                → Star schema (Kimball)
│       ├── _gold.yml
│       ├── dim_customers.sql   → Customer dimension (SCD2)
│       ├── dim_products.sql    → Product dimension (SCD2)
│       └── fact_sales.sql      → Sales fact table
│
├── tests/                   → Custom DQ tests
│   ├── assert_no_orphaned_customer_keys.sql
│   ├── assert_no_orphaned_product_keys.sql
│   ├── assert_no_overlapping_customer_dates.sql
│   ├── assert_single_current_customer.sql
│   └── assert_single_current_product.sql
│
└── macros/
    └── scd2_helpers.sql     → SCD2 utility macros
```

## 🔄 Data Lineage

```
bronze_dw.crm_cust_info    ──→ int_crm_cust_info ──┐
bronze_dw.erp_cust_az12    ──→ int_erp_cust_az12 ──┼──→ int_customers_enriched ──→ dim_customers ──┐
bronze_dw.erp_loc_a101     ──→ int_erp_loc_a101  ──┘                                                │
                                                                                                     ├──→ fact_sales
bronze_dw.crm_prd_info     ──→ int_crm_prd_info     ──┐                                             │
bronze_dw.erp_px_cat_g1v2  ──→ int_erp_px_cat_g1v2 ──┴──→ int_products_enriched ──→ dim_products ──┘
                                                                                         ↑
bronze_dw.crm_sales_details ──→ int_sales ───────────────────────────────────────────────
```

## 🚀 Quick Start Commands

```bash
# 1. Kích hoạt môi trường
source .venv/bin/activate

# 2. Cài packages (1 lần duy nhất)
dbt deps

# 3. Kiểm tra syntax (không cần DB)
dbt parse --profiles-dir .

# 4. Chạy tất cả models (cần MySQL)
dbt run --profiles-dir .

# 5. Chạy tests
dbt test --profiles-dir .

# 6. Xem documentation
dbt docs generate --no-compile --profiles-dir .
dbt docs serve --port 8080 --profiles-dir .
# → Mở http://localhost:8080
```

## 🎯 So sánh MySQL gốc vs dbt

| MySQL | dbt |
|---|---|
| `01_ddl_silver.sql` (CREATE TABLE) | Tự động bởi `materialized = 'incremental'` |
| `02_load_silver.sql` (INSERT + upsert) | Logic trong từng `int_*.sql` model |
| `ON DUPLICATE KEY UPDATE` | `unique_key` trong model config |
| `WHERE date > @watermark` | `{% if is_incremental() %}` block |
| `ROW_NUMBER() OVER (PARTITION BY ...)` | `qualify row_number()...` CTE |
| `ctrl_watermark` table | `select max(...) from {{ this }}` |
| `ctrl_dq_log` table | dbt `tests` trong `_intermediate.yml` |
| `ctrl_pipeline_log` table | dbt built-in run logs |

## 🔐 Production Considerations

### 1. **Connection Security**
```yaml
# Dùng environment variables thay vì hardcode
profiles.yml:
  outputs:
    prod:
      type: mysql
      host: "{{ env_var('DBT_MYSQL_HOST') }}"
      user: "{{ env_var('DBT_MYSQL_USER') }}"
      password: "{{ env_var('DBT_MYSQL_PASSWORD') }}"
```

### 2. **Scheduling**
- Airflow DAG for orchestration
- Run incremental mỗi giờ/ngày
- Full refresh hàng tuần/tháng

### 3. **Monitoring**
- dbt Cloud for run monitoring
- Elementary for data quality observability

## 🎓 Learning Resources

- [dbt Documentation](https://docs.getdbt.com)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [SCD Type 2 Explained](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row)

---

**Tổng kết:** Data warehouse 3 layers (Bronze → Silver → Gold) được migrate hoàn toàn từ MySQL sang dbt, giữ nguyên logic business nhưng tận dụng dependency graph, incremental loading, và data quality tests tự động của dbt. 🎉


```
┌─────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                         │
│                     (Raw Data - MySQL)                      │
├─────────────────────────────────────────────────────────────┤
│  CRM System          │  ERP System                          │
│  • crm_cust_info     │  • erp_cust_az12 (attributes)       │
│  • crm_prd_info      │  • erp_loc_a101 (country)           │
│  • crm_sales_details │  • erp_px_cat_g1v2 (category)       │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                       SILVER LAYER                          │
│                 (Cleaned & Transformed)                     │
├─────────────────────────────────────────────────────────────┤
│  Staging Models (stg_*)                                     │
│  • Clean NULL values                                        │
│  • Standardize formats (UPPER, TRIM)                        │
│  • Add metadata (ingestion_ts, batch_id)                    │
│                                                              │
│  Intermediate Models (int_*)                                │
│  • int_customers_enriched (CRM + ERP + Country)            │
│  • int_products_enriched (Products + Categories)            │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                           │
│                  (Star Schema - Kimball)                    │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐         ┌──────────────────┐         │
│  │ dim_customers    │         │  dim_products    │         │
│  ├──────────────────┤         ├──────────────────┤         │
│  │ customer_key (PK)│         │ product_key (PK) │         │
│  │ customer_id (BK) │         │ product_id (BK)  │         │
│  │ first_name       │         │ product_name     │         │
│  │ last_name        │         │ category_name    │         │
│  │ country_name     │         │ parent_category  │         │
│  │ segment          │         │ valid_from       │         │
│  │ valid_from       │         │ valid_to         │         │
│  │ valid_to         │         │ is_current       │         │
│  │ is_current       │         └──────────────────┘         │
│  └──────────────────┘                  │                    │
│           │                            │                    │
│           │         ┌─────────────────────────────┐         │
│           └────────→│     fact_sales              │←────────┘
│                     ├─────────────────────────────┤         │
│                     │ sales_key (PK)              │         │
│                     │ customer_key (FK)           │         │
│                     │ product_key (FK)            │         │
│                     │ order_number                │         │
│                     │ order_date                  │         │
│                     │ quantity                    │         │
│                     │ amount                      │         │
│                     │ total_amount                │         │
│                     └─────────────────────────────┘         │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## 🎯 Key Features

### 1. **Slowly Changing Dimension Type 2 (SCD2)**

Cả `dim_customers` và `dim_products` implement SCD2 để track lịch sử thay đổi:

**Example: Customer changes country**
```
customer_id | customer_key | country | valid_from  | valid_to    | is_current
------------|--------------|---------|-------------|-------------|------------
123         | ABC-2024-01  | USA     | 2024-01-01  | 2024-06-15  | false
123         | ABC-2024-06  | CANADA  | 2024-06-15  | 9999-12-31  | true
```

### 2. **Point-in-Time Correct Joins**

Fact table joins với dimension dựa trên `order_date`:

```sql
-- Get customer version valid at order time
LEFT JOIN dim_customers c
    ON f.customer_id = c.customer_id
    AND f.order_date >= c.valid_from
    AND f.order_date < c.valid_to
```

### 3. **Incremental Loading**

- **Dimensions**: Compare hash để detect changes, chỉ update khi có thay đổi
- **Fact**: Load only new orders (orders > max existing order_date)

### 4. **Data Quality Tests**

Automatic tests cho:
- ✅ Primary key uniqueness
- ✅ Not null constraints  
- ✅ Referential integrity (FK relationships)
- ✅ Custom business rules

## 📁 File Structure & Purpose

```
dw_dbt/
│
├── dbt_project.yml          → Project config, materialization strategy
├── packages.yml             → dbt packages (utils, expectations)
├── profiles.yml             → MySQL connection (LOCAL, do not commit!)
│
├── models/
│   ├── sources/
│   │   └── _sources.yml     → Define bronze_dw tables as sources
│   │
│   ├── staging/             → Bronze → Silver transformation
│   │   ├── _staging.yml     → Model docs & tests
│   │   ├── stg_crm_cust_info.sql        → Clean customers
│   │   ├── stg_crm_prd_info.sql         → Clean products
│   │   ├── stg_crm_sales_details.sql    → Clean sales
│   │   ├── stg_erp_cust_az12.sql        → Clean customer attrs
│   │   ├── stg_erp_loc_a101.sql         → Clean countries
│   │   └── stg_erp_px_cat_g1v2.sql      → Clean categories
│   │
│   ├── intermediate/        → Silver enrichment layer
│   │   ├── _intermediate.yml
│   │   ├── int_customers_enriched.sql   → Customers + ERP + Country
│   │   └── int_products_enriched.sql    → Products + Categories
│   │
│   └── gold/                → Star schema (Kimball)
│       ├── _gold.yml        → Dimension & fact docs/tests
│       ├── dim_customers.sql   → Customer dimension (SCD2)
│       ├── dim_products.sql    → Product dimension (SCD2)
│       └── fact_sales.sql      → Sales fact table
│
├── macros/
│   └── scd2_helpers.sql     → SCD2 utility macros
│
├── SETUP_GUIDE.md           → Complete setup instructions
├── example_queries.md       → Sample analytical queries
└── run_dbt.sh               → Helper script for common operations
```

## 🔄 Data Lineage

```
bronze_dw.crm_cust_info  ─┐
                          ├──→ stg_crm_cust_info ─┐
bronze_dw.erp_cust_az12  ─┤                       ├──→ int_customers_enriched ──→ dim_customers ─┐
                          └──→ stg_erp_cust_az12 ─┤                                               │
bronze_dw.erp_loc_a101   ────→ stg_erp_loc_a101  ─┘                                               │
                                                                                                   │
bronze_dw.crm_prd_info   ─┬──→ stg_crm_prd_info ─┬──→ int_products_enriched ──→ dim_products ──┐ │
bronze_dw.erp_px_cat_g1v2 ─┴──→ stg_erp_px_cat_g1v2 ─┘                                          │ │
                                                                                                  │ │
bronze_dw.crm_sales_details ──→ stg_crm_sales_details ──────────────────────────────────────────┼─┼──→ fact_sales
                                                                                                  │ │
                                                                                                  └─┘
```

## 🚀 Quick Start Commands

```bash
# 1. Install dependencies
dbt deps

# 2. Test connection
dbt debug

# 3. Initial load (full refresh)
dbt run --full-refresh

# 4. Run tests
dbt test

# 5. View documentation
dbt docs generate && dbt docs serve

# 6. Incremental updates (daily/hourly)
dbt run
```

Or use helper script:
```bash
./run_dbt.sh all          # Complete pipeline
./run_dbt.sh staging      # Run only staging
./run_dbt.sh gold         # Run only gold
./run_dbt.sh dims         # Run only dimensions
./run_dbt.sh facts        # Run only facts
```

## 📊 Example Analytics Queries

### 1. Top 10 Customers by Revenue
```sql
SELECT 
    c.customer_number,
    c.first_name,
    c.last_name,
    c.country_name,
    SUM(f.total_amount) as total_revenue
FROM gold_dw.fact_sales f
JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY 1,2,3,4
ORDER BY total_revenue DESC
LIMIT 10;
```

### 2. Monthly Sales Trend
```sql
SELECT 
    DATE_FORMAT(order_date, '%Y-%m') as month,
    COUNT(DISTINCT order_number) as total_orders,
    SUM(total_amount) as total_revenue
FROM gold_dw.fact_sales
GROUP BY 1
ORDER BY 1 DESC;
```

### 3. Track Customer Segment Changes (SCD2)
```sql
SELECT 
    customer_id,
    customer_number,
    segment,
    valid_from,
    valid_to,
    is_current
FROM gold_dw.dim_customers
WHERE customer_id = 123
ORDER BY valid_from;
```

## 🎯 Benefits of This Architecture

### ✅ **Separation of Concerns**
- Bronze: Raw data (immutable)
- Silver: Business logic & transformations
- Gold: Analytics-ready star schema

### ✅ **Historical Tracking (SCD2)**
- Track ALL changes to customers & products
- Point-in-time accurate reporting
- Audit trail for compliance

### ✅ **Performance**
- Incremental loads (only process new data)
- Star schema optimized for analytics
- Indexes on fact table FKs

### ✅ **Data Quality**
- Automated tests on every run
- Source freshness checks
- Referential integrity validation

### ✅ **Documentation**
- Auto-generated lineage graphs
- Column-level descriptions
- Test results tracking

### ✅ **Version Control**
- All SQL in Git
- Reproducible builds
- Easy rollbacks

## 🔐 Production Considerations

### 1. **Connection Security**
```yaml
# Use environment variables
profiles.yml:
  outputs:
    prod:
      type: mysql
      host: "{{ env_var('DBT_MYSQL_HOST') }}"
      user: "{{ env_var('DBT_MYSQL_USER') }}"
      password: "{{ env_var('DBT_MYSQL_PASSWORD') }}"
```

### 2. **Scheduling**
- Airflow DAG for orchestration
- Run incremental every hour/day
- Full refresh weekly/monthly

### 3. **Monitoring**
- dbt Cloud for run monitoring
- Elementary for data quality observability
- Custom alerts on test failures

### 4. **Performance Tuning**
- Add indexes to bronze tables
- Partition large fact tables by date
- Increase threads in profiles.yml

## 📈 Scaling the Warehouse

### Add More Dimensions
```sql
-- dim_date.sql (date dimension)
-- dim_location.sql (geography)
-- dim_time.sql (time of day)
```

### Add More Facts
```sql
-- fact_inventory.sql
-- fact_customer_activity.sql
-- fact_product_performance.sql
```

### Add Data Quality Framework
```yaml
# Use dbt_expectations
tests:
  - dbt_expectations.expect_column_values_to_be_between:
      min_value: 0
      max_value: 1000000
```

## 🎓 Learning Resources

- [dbt Documentation](https://docs.getdbt.com)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [SCD Type 2 Explained](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row)

---

**Tổng kết:** Bạn đã có một data warehouse hoàn chỉnh với dbt, implement Kimball star schema, SCD2, incremental loading, data quality tests, và full documentation! 🎉
