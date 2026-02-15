-- ============================================================================
-- script/03_gold/01_ddl_gold.sql
-- Purpose:
--   Gold layer provides analytics-ready dimensional model (star schema).
--
-- Notes:
--   - Watermark / pipeline log / dq log live in control_dw (not created here).
--   - Unique keys enable upserts by business keys (ON DUPLICATE KEY UPDATE).
--   - Timestamps use DATETIME(6) for microseconds.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold_dw;

-- ----------------------------------------------------------------------------
-- DIM_CUSTOMERS
-- Grain:
--   1 row = 1 customer (Type-1 / current view)
-- Keys:
--   - Surrogate key: customer_key
--   - Business key (unique): customer_id (from CRM)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold_dw.dim_customers;

CREATE TABLE gold_dw.dim_customers (
  customer_key      BIGINT AUTO_INCREMENT PRIMARY KEY,  -- surrogate key

  customer_id       INT          NOT NULL,              -- business key (cst_id)
  customer_number   VARCHAR(50)  NULL,                  -- cst_key

  first_name        VARCHAR(100) NULL,
  last_name         VARCHAR(100) NULL,
  country           VARCHAR(100) NULL,
  marital_status    VARCHAR(20)  NULL,
  gender            VARCHAR(10)  NULL,
  birthdate         DATE         NULL,
  create_date       DATE         NULL,

  -- Technical metadata (from Silver)
  src_ingestion_ts  DATETIME(6)  NULL,

  -- Enables upsert by business key
  UNIQUE KEY uk_dim_customers_customer_id (customer_id),

  -- Optional helpful index
  INDEX idx_dim_customers_customer_number (customer_number)
);

-- ----------------------------------------------------------------------------
-- DIM_PRODUCTS
-- Grain:
--   1 row = 1 product (active/current, Type-1)
-- Keys:
--   - Surrogate key: product_key
--   - Business key (unique): product_number (prd_key)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold_dw.dim_products;

CREATE TABLE gold_dw.dim_products (
  product_key       BIGINT AUTO_INCREMENT PRIMARY KEY,  -- surrogate key

  product_id        INT          NOT NULL,              -- prd_id
  product_number    VARCHAR(50)  NULL,                  -- prd_key (business key)
  product_name      VARCHAR(255) NULL,

  category_id       VARCHAR(50)  NULL,
  category          VARCHAR(255) NULL,
  subcategory       VARCHAR(255) NULL,
  maintenance       VARCHAR(255) NULL,

  cost              DECIMAL(18,2) NULL,
  product_line      VARCHAR(50)   NULL,
  start_date        DATE          NULL,

  -- Technical metadata (from Silver)
  src_ingestion_ts  DATETIME(6)   NULL,

  -- Enables upsert by business key
  UNIQUE KEY uk_dim_products_product_number (product_number),

  INDEX idx_dim_products_category (category_id)
);

-- ----------------------------------------------------------------------------
-- FACT_SALES
-- Grain:
--   1 row = 1 order line (order_number + line_number)
-- Keys:
--   - Business key (unique): (order_number, line_number)
--   - Foreign keys (logical): product_key -> dim_products.product_key
--                            customer_key -> dim_customers.customer_key
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold_dw.fact_sales;

CREATE TABLE gold_dw.fact_sales (
  -- Business key
  order_number      VARCHAR(50) NOT NULL,
  line_number       INT         NOT NULL,

  -- Dim surrogate keys
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

  -- Upsert key
  UNIQUE KEY uk_fact_sales_order_line (order_number, line_number),

  -- Helpful indexes for analytics queries
  INDEX idx_fact_sales_order_date (order_date),
  INDEX idx_fact_sales_customer (customer_key),
  INDEX idx_fact_sales_product (product_key)
);

-- ALTER TABLE gold_dw.fact_sales
--   ADD CONSTRAINT fk_fact_sales_product
--     FOREIGN KEY (product_key) REFERENCES gold_dw.dim_products(product_key),
--   ADD CONSTRAINT fk_fact_sales_customer
--     FOREIGN KEY (customer_key) REFERENCES gold_dw.dim_customers(customer_key);
