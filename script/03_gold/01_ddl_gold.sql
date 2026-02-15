-- ============================================================================
-- gold_dw.ddl_gold.sql
-- Purpose:
--   Gold layer provides analytics-ready dimensional model (star schema).
--
-- Notes:
--   - Watermark/logging tables live in control_dw (not created here).
--   - Unique keys enable upserts by business keys.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- FACT_SALES
-- Grain:
--   1 row = 1 order line (order_number + line_number)
-- Keys:
--   - Business key: (order_number, line_number)
--   - Foreign keys: product_key -> dim_products.product_key
--                 customer_key -> dim_customers.customer_key
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold_dw.fact_sales;

CREATE TABLE gold_dw.fact_sales (
  -- Business key (recommended)
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