# 🗺 Data Warehouse Lineage & Data Flow

## Complete Data Lineage Graph

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            BRONZE LAYER (MySQL)                         │
│                          Source: bronze_dw schema                       │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         │                    │                    │
    ┌────▼────┐          ┌────▼────┐          ┌────▼────┐
    │  CRM    │          │  CRM    │          │  CRM    │
    │ cust_   │          │  prd_   │          │ sales_  │
    │ info    │          │ info    │          │ details │
    └────┬────┘          └────┬────┘          └────┬────┘
         │                    │                    │
         │              ┌─────▼─────┐              │
    ┌────▼────┐         │   ERP     │         ┌────▼────┐
    │  ERP    │         │ px_cat_   │         │         │
    │ cust_   │         │  g1v2     │         │         │
    │  az12   │         └─────┬─────┘         │         │
    └────┬────┘               │               │         │
         │                    │               │         │
    ┌────▼────┐               │               │         │
    │  ERP    │               │               │         │
    │ loc_    │               │               │         │
    │  a101   │               │               │         │
    └────┬────┘               │               │         │
         │                    │               │         │
┌────────┴────────────────────┴───────────────┴─────────┴────────────────┐
│                         STAGING LAYER (dbt)                             │
│                      Materialized: VIEW in silver_dw                    │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         │                    │                    │
    ┌────▼────────┐      ┌────▼────────┐      ┌────▼────────┐
    │    stg_     │      │    stg_     │      │    stg_     │
    │   crm_      │      │   crm_      │      │   crm_      │
    │   cust_     │      │   prd_      │      │   sales_    │
    │   info      │      │   info      │      │   details   │
    └────┬────────┘      └────┬────────┘      └────┬────────┘
         │                    │                    │
    ┌────▼────────┐      ┌────▼────────┐          │
    │    stg_     │      │    stg_     │          │
    │   erp_      │      │   erp_      │          │
    │   cust_     │      │   px_cat_   │          │
    │   az12      │      │   g1v2      │          │
    └────┬────────┘      └────┬────────┘          │
         │                    │                    │
    ┌────▼────────┐           │                    │
    │    stg_     │           │                    │
    │   erp_      │           │                    │
    │   loc_      │           │                    │
    │   a101      │           │                    │
    └────┬────────┘           │                    │
         │                    │                    │
┌────────┴────────────────────┴────────────────────┴─────────────────────┐
│                      INTERMEDIATE LAYER (dbt)                           │
│                   Materialized: VIEW in silver_dw                       │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         │                    │                    │
         └──────┬─────────────┘                    │
                │                                  │
         ┌──────▼──────┐                           │
         │    int_     │                           │
         │ customers_  │                           │
         │ enriched    │                           │
         └──────┬──────┘                           │
                │                                  │
                │              ┌───────▼───────┐   │
                │              │     int_      │   │
                │              │  products_    │   │
                │              │  enriched     │   │
                │              └───────┬───────┘   │
                │                      │           │
┌───────────────┴──────────────────────┴───────────┴──────────────────────┐
│                          GOLD LAYER (dbt)                                │
│                 Materialized: INCREMENTAL in gold_dw                     │
└──────────────────────────────────────────────────────────────────────────┘
                │                      │           │
         ┌──────▼──────┐        ┌──────▼──────┐   │
         │    dim_     │        │    dim_     │   │
         │ customers   │        │  products   │   │
         │  (SCD2)     │        │   (SCD2)    │   │
         └──────┬──────┘        └──────┬──────┘   │
                │                      │           │
                │      ┌───────────────┼───────────┘
                │      │               │
                └──────┼───────┬───────┘
                       │       │
                    ┌──▼───────▼──┐
                    │   fact_     │
                    │   sales     │
                    └─────────────┘
```

---

## Layer-by-Layer Transformation

### 🥉 BRONZE → SILVER: Staging Layer

**Purpose:** Clean, standardize, validate

```
crm_cust_info (Raw)                  stg_crm_cust_info (Cleaned)
─────────────────────                ────────────────────────────
cst_id: 123                          cst_id: 123
cst_firstname: " john "       →      cst_firstname: "JOHN"
cst_lastname: "doe  "                cst_lastname: "DOE"
cst_create_date: 2024-01-01          cst_create_date: 2024-01-01
                                     ingestion_ts: 2024-03-02 10:00:00
                                     batch_id: abc-123
```

**Transformations:**
- ✅ `TRIM()` whitespace
- ✅ `UPPER()` names
- ✅ Remove NULLs
- ✅ Add metadata (ingestion_ts, batch_id)

---

### 🥈 SILVER: Intermediate Layer

**Purpose:** Enrich, join, business logic

```
stg_crm_cust_info + stg_erp_cust_az12 + stg_erp_loc_a101
     │                    │                    │
     │    ┌───────────────┘                    │
     │    │    ┌───────────────────────────────┘
     └────┼────┘
          ↓
    int_customers_enriched
    ──────────────────────
    cst_id: 123
    cst_firstname: "JOHN"
    cst_lastname: "DOE"
    country_code: "US"          ← From erp_cust_az12
    country_name: "United States" ← From erp_loc_a101
    region: "North America"     ← From erp_loc_a101
    segment: "Premium"          ← From erp_cust_az12
```

**Transformations:**
- ✅ LEFT JOIN multiple sources
- ✅ Enrich with ERP attributes
- ✅ Add derived fields

---

### 🥇 GOLD: Dimensional Layer

#### Dimension with SCD Type 2

```
int_customers_enriched                dim_customers (SCD2)
──────────────────────                ─────────────────────
cst_id: 123                           customer_key: ABC-001-V1
cst_firstname: "JOHN"          →      customer_id: 123 (business key)
cst_lastname: "DOE"                   first_name: "JOHN"
country_code: "US"                    last_name: "DOE"
country_name: "United States"         country_code: "US"
segment: "Premium"                    country_name: "United States"
                                      segment: "Premium"
                                      valid_from: 2024-01-01
                                      valid_to: 9999-12-31
                                      is_current: true

When customer moves to CANADA:
                                      customer_key: ABC-001-V2
                                      customer_id: 123
                                      country_code: "CA"
                                      country_name: "Canada"
                                      valid_from: 2024-06-15
                                      valid_to: 9999-12-31
                                      is_current: true

                                      (Previous version closed)
                                      customer_key: ABC-001-V1
                                      valid_to: 2024-06-15
                                      is_current: false
```

#### Fact Table with Point-in-Time Joins

```
stg_crm_sales_details          dim_customers          dim_products
─────────────────────          ─────────────          ────────────
sls_id: 1                      customer_key: ABC-V1   product_key: PRD-V1
sls_order_number: ORD-001      customer_id: 123       product_id: 456
sls_cust_id: 123        ─────→ valid_from: 2024-01-01 valid_from: 2024-01-01
sls_prd_key: 456        ─────→ valid_to: 2024-06-15   valid_to: 9999-12-31
sls_order_date: 2024-03-01     is_current: false      is_current: true
sls_quantity: 5
sls_amount: 100
                                        │
                                        ↓
                                  fact_sales
                                  ──────────
                                  sales_key: FACT-001
                                  customer_key: ABC-V1  ← USA version (at order_date)
                                  product_key: PRD-V1
                                  order_number: ORD-001
                                  order_date: 2024-03-01
                                  quantity: 5
                                  amount: 100
                                  total_amount: 500

JOIN Logic:
WHERE order_date >= dim.valid_from AND order_date < dim.valid_to
      ↑
      Ensures correct dimension version at time of transaction!
```

---

## Incremental Load Strategy

### Initial Load (--full-refresh)
```
1. Staging: Load all rows from bronze
2. Intermediate: Join all data
3. Dimensions: Create all records with is_current = true
4. Fact: Load all sales transactions
```

### Incremental Load (dbt run)
```
1. Staging: Load all rows (views, always fresh)
2. Intermediate: Join all data (views, always fresh)
3. Dimensions (SCD2):
   - Compare hash of new data vs existing
   - If changed:
     → Close old version (is_current = false, valid_to = now)
     → Create new version (is_current = true, valid_to = 9999-12-31)
   - If unchanged: Keep as is
   - If new: Create new record
4. Fact:
   - Load only new sales (order_date > max existing order_date)
   - Join with dimension versions valid at order_date
```

---

## Test Coverage Map

```
┌──────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                            │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  dim_customers                  dim_products                 │
│  ─────────────                  ────────────                 │
│  Tests:                         Tests:                       │
│  ✓ unique(customer_key)         ✓ unique(product_key)        │
│  ✓ not_null(customer_key)       ✓ not_null(product_key)      │
│  ✓ not_null(valid_from)         ✓ not_null(valid_from)       │
│  ✓ not_null(valid_to)           ✓ not_null(valid_to)         │
│  ✓ not_null(is_current)         ✓ not_null(is_current)       │
│                                                               │
│  Custom Tests:                  Custom Tests:                │
│  ✓ single_current_customer      ✓ single_current_product     │
│  ✓ no_overlapping_dates                                      │
│                                                               │
│  fact_sales                                                  │
│  ───────────                                                 │
│  Tests:                                                      │
│  ✓ unique(sales_key)                                         │
│  ✓ not_null(sales_key)                                       │
│  ✓ not_null(customer_key)                                    │
│  ✓ not_null(product_key)                                     │
│  ✓ not_null(order_date)                                      │
│  ✓ relationships(customer_key → dim_customers)               │
│  ✓ relationships(product_key → dim_products)                 │
│                                                               │
│  Custom Tests:                                               │
│  ✓ no_orphaned_customer_keys                                 │
│  ✓ no_orphaned_product_keys                                  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

## Performance Optimization Points

```
┌──────────────────────────────────────────────────────────────┐
│               RECOMMENDED INDEXES                             │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  bronze_dw (Source Tables):                                  │
│  ─────────────────────────                                   │
│  CREATE INDEX idx_cust_id ON crm_cust_info(cst_id);          │
│  CREATE INDEX idx_prd_id ON crm_prd_info(prd_id);            │
│  CREATE INDEX idx_sales_date ON crm_sales_details(sls_order_date); │
│                                                               │
│  gold_dw (Dimensions):                                       │
│  ──────────────────────                                      │
│  CREATE INDEX idx_dim_cust_id ON dim_customers(customer_id);  │
│  CREATE INDEX idx_dim_cust_current ON dim_customers(is_current); │
│  CREATE INDEX idx_dim_cust_dates ON dim_customers(valid_from, valid_to); │
│                                                               │
│  CREATE INDEX idx_dim_prod_id ON dim_products(product_id);    │
│  CREATE INDEX idx_dim_prod_current ON dim_products(is_current); │
│                                                               │
│  gold_dw (Facts):                                            │
│  ─────────────────                                           │
│  CREATE INDEX idx_fact_cust_key ON fact_sales(customer_key);  │
│  CREATE INDEX idx_fact_prod_key ON fact_sales(product_key);   │
│  CREATE INDEX idx_fact_order_date ON fact_sales(order_date);  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

**Total Pipeline:** 6 staging → 2 intermediate → 3 gold = **11 models**

**Execution Order:** Staging → Intermediate → Dimensions → Facts

**Expected Runtime:** 5-15 minutes (depending on data volume)

---

*Visualized by GitHub Copilot* 🎨
