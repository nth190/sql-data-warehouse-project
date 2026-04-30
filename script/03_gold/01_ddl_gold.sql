-- ============================================================================
-- script/03_gold/01_ddl_gold.sql
-- Purpose:
--   Gold layer provides analytics-ready dimensional model (star schema).
--
-- Design:
--   - Type-1 dimensions (current state only)
--   - Fact table at order line grain
--   - Surrogate keys in dimensions
--   - Business keys used for upsert logic
--
-- Notes:
--   - Watermark / pipeline / DQ logs live in control_dw
--   - DATETIME(6) used for microsecond precision
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold_dw;
USE gold_dw;

-- ============================================================================
-- GOLD DIMENSION: gold_dw.dim_customers (SCD Type 2)
-- Grain:
--   1 row = 1 version of a customer (current or historical)
-- Keys:
--   - Surrogate key: customer_key (PK)
--   - Natural/business key: customer_id (from CRM)
--   - SCD2 columns: effective_start_date, effective_end_date, is_current
-- ============================================================================


DROP TABLE IF EXISTS gold_dw.dim_customers;

CREATE TABLE gold_dw.dim_customers (
  -- Surrogate key (technical primary key)
  customer_key        BIGINT AUTO_INCREMENT PRIMARY KEY,

  -- Natural/business key from source systems
  customer_id         INT          NOT NULL,         -- maps to silver_dw.crm_cust_info.cst_id
  customer_number     VARCHAR(50)  NULL,             -- cst_key

  -- Descriptive attributes
  first_name          VARCHAR(100) NULL,
  last_name           VARCHAR(100) NULL,
  country             VARCHAR(100) NULL,
  marital_status      VARCHAR(20)  NULL,
  gender              VARCHAR(10)  NULL,
  birthdate           DATE         NULL,
  create_date         DATE         NULL,

  -- SCD Type 2 tracking
  effective_start_date DATE        NOT NULL,         -- when this version became active
  effective_end_date   DATE        NOT NULL,         -- when this version stopped being active
  is_current           TINYINT(1)  NOT NULL DEFAULT 1,  -- 1 = current, 0 = historical

  -- Technical metadata (for lineage/troubleshooting)
  src_ingestion_ts    DATETIME(6)  NULL,

  -- Helpful indexes
  INDEX idx_dim_customers_nk_current (customer_id, is_current),
  INDEX idx_dim_customers_nk_range   (customer_id, effective_start_date, effective_end_date),
  INDEX idx_dim_customers_number     (customer_number)
);


-- ============================================================================
-- GOLD DIMENSION: gold_dw.dim_products (SCD Type 2)
-- Grain:
--   1 row = 1 version of a product (current or historical)
-- Keys:
--   - Surrogate key: product_key (PK)
--   - Natural/business key: product_number (from CRM)
--   - SCD2 columns: effective_start_date, effective_end_date, is_current
-- ============================================================================

USE gold_dw;

DROP TABLE IF EXISTS gold_dw.dim_products;

CREATE TABLE gold_dw.dim_products (
  product_key          BIGINT AUTO_INCREMENT PRIMARY KEY,
  product_id           INT          NOT NULL,
  product_number       VARCHAR(50)  NOT NULL,

  product_name         VARCHAR(255) NULL,
  category_id          VARCHAR(50)  NULL,
  category             VARCHAR(255) NULL,
  subcategory          VARCHAR(255) NULL,
  maintenance          VARCHAR(255) NULL,

  cost                 DECIMAL(18,2) NULL,
  product_line         VARCHAR(50)   NULL,
  start_date           DATE          NULL,

  -- SCD2 tracking
  effective_start_date DATE         NOT NULL,
  effective_end_date   DATE         NOT NULL,
  is_current           TINYINT(1)   NOT NULL DEFAULT 1,

  src_ingestion_ts     DATETIME(6)  NULL,

  UNIQUE KEY uk_dim_products_product_number (product_number, effective_start_date),
  INDEX idx_dim_products_nk_current (product_number, is_current),
  INDEX idx_dim_products_nk_range (product_number, effective_start_date, effective_end_date)
);

-- ----------------------------------------------------------------------------
-- FACT_SALES (no line_number version)
-- Grain:
--   1 row = 1 order_number + 1 product + 1 customer
-- Keys:
--   - Business key (unique): (order_number, product_key, customer_key)
--   - Foreign keys (logical): product_key -> dim_products.product_key
--                             customer_key -> dim_customers.customer_key
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold_dw.fact_sales;

CREATE TABLE gold_dw.fact_sales (
  -- Business key
  order_number      VARCHAR(50) NOT NULL,
  product_key       BIGINT      NOT NULL,
  customer_key      BIGINT      NOT NULL,

  -- Dates
  order_date        DATE        NULL,
  shipping_date     DATE        NULL,
  due_date          DATE        NULL,

  -- Measures
  sales_amount      DECIMAL(18,2) NULL,
  quantity          INT           NULL,
  price             DECIMAL(18,2) NULL,

  -- Technical metadata (from Silver)
  src_ingestion_ts  DATETIME(6)  NULL,

  -- Upsert key: 1 order x 1 product x 1 customer
  UNIQUE KEY uk_fact_sales_ord_prod_cust (order_number, product_key, customer_key),

  -- Helpful indexes for analytics queries
  INDEX idx_fact_sales_order_date (order_date),
  INDEX idx_fact_sales_customer (customer_key),
  INDEX idx_fact_sales_product (product_key)
);
SHOW FULL COLUMNS FROM gold_dw.dim_customers;