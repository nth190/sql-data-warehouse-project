/* =============================================================================
   GOLD LAYER - INCREMENTAL LOAD (Watermark + Upsert + Logging)
   Targets:
     - gold_dw.dim_customers  (wm = ci.cst_create_date)
     - gold_dw.dim_products   (wm = pn.prd_start_dt)
     - gold_dw.fact_sales     (wm = sd.ingestion_ts)

   Control tables (created once in control_dw):
     - control_dw.ctrl_watermark
     - control_dw.ctrl_pipeline_log
     - control_dw.ctrl_dq_log
============================================================================= */

SET @run_ts := NOW(6);
SET @run_id := DATE_FORMAT(@run_ts, '%Y%m%d%H%i%s%f');

-- =============================================================================
-- A) DIM_CUSTOMERS (wm = cst_create_date)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'gold_dw.dim_customers'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'GOLD', 'gold_dw.dim_customers', 'STARTED', @run_ts, CONCAT('watermark=', @wm));

INSERT INTO gold_dw.dim_customers
(customer_id, customer_number, first_name, last_name, country, marital_status, gender, birthdate, create_date, src_ingestion_ts)
SELECT
  ci.cst_id,
  ci.cst_key,
  ci.cst_firstname,
  ci.cst_lastname,
  la.cntry,
  ci.cst_marital_status,
  CASE
    WHEN ci.cst_gndr IS NULL THEN 'n/a'
    WHEN ca.gen IS NULL OR ca.gen = 'n/a' THEN ci.cst_gndr
    WHEN ci.cst_gndr <> ca.gen THEN ca.gen
    ELSE COALESCE(ca.gen, 'n/a')
  END,
  ca.bdate,
  ci.cst_create_date,
  ci.ingestion_ts
FROM silver_dw.crm_cust_info ci
LEFT JOIN silver_dw.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver_dw.erp_loc_a101 la ON ci.cst_key = la.cid
WHERE ci.cst_create_date IS NOT NULL
  AND ci.cst_create_date > @wm
ON DUPLICATE KEY UPDATE
  customer_number  = VALUES(customer_number),
  first_name       = VALUES(first_name),
  last_name        = VALUES(last_name),
  country          = VALUES(country),
  marital_status   = VALUES(marital_status),
  gender           = VALUES(gender),
  birthdate        = VALUES(birthdate),
  create_date      = VALUES(create_date),
  src_ingestion_ts = VALUES(src_ingestion_ts);

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(ci.cst_create_date) FROM silver_dw.crm_cust_info ci WHERE ci.cst_create_date IS NOT NULL AND ci.cst_create_date > @wm),
  @wm
)
WHERE table_name = 'gold_dw.dim_customers';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'GOLD',
  'gold_dw.dim_customers',
  'customer_id_not_null',
  CASE WHEN SUM(CASE WHEN ci.cst_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN ci.cst_id IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM silver_dw.crm_cust_info ci
WHERE ci.cst_create_date IS NOT NULL
  AND ci.cst_create_date > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'GOLD', 'gold_dw.dim_customers', 'FINISHED', NOW(6), @rows, 'upsert completed');

-- =============================================================================
-- B) DIM_PRODUCTS (wm = prd_start_dt)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'gold_dw.dim_products'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'GOLD', 'gold_dw.dim_products', 'STARTED', NOW(6), CONCAT('watermark=', @wm));

INSERT INTO gold_dw.dim_products
(product_id, product_number, product_name, category_id, category, subcategory, maintenance, cost, product_line, start_date, src_ingestion_ts)
SELECT
  pn.prd_id,
  pn.prd_key,
  pn.prd_nm,
  pn.cat_id,
  pc.cat,
  pc.subcat,
  pc.maintenance,
  pn.prd_cost,
  pn.prd_line,
  pn.prd_start_dt,
  pn.ingestion_ts
FROM silver_dw.crm_prd_info pn
LEFT JOIN silver_dw.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL
  AND pn.prd_start_dt IS NOT NULL
  AND pn.prd_start_dt > @wm
ON DUPLICATE KEY UPDATE
  product_name     = VALUES(product_name),
  category_id      = VALUES(category_id),
  category         = VALUES(category),
  subcategory      = VALUES(subcategory),
  maintenance      = VALUES(maintenance),
  cost             = VALUES(cost),
  product_line     = VALUES(product_line),
  start_date       = VALUES(start_date),
  src_ingestion_ts = VALUES(src_ingestion_ts);

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(pn.prd_start_dt)
    FROM silver_dw.crm_prd_info pn
    WHERE pn.prd_end_dt IS NULL
      AND pn.prd_start_dt IS NOT NULL
      AND pn.prd_start_dt > @wm
  ),
  @wm
)
WHERE table_name = 'gold_dw.dim_products';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'GOLD',
  'gold_dw.dim_products',
  'product_number_not_null',
  CASE WHEN SUM(CASE WHEN pn.prd_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN pn.prd_key IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM silver_dw.crm_prd_info pn
WHERE pn.prd_end_dt IS NULL
  AND pn.prd_start_dt IS NOT NULL
  AND pn.prd_start_dt > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'GOLD', 'gold_dw.dim_products', 'FINISHED', NOW(6), @rows, 'upsert completed');

-- =============================================================================
-- C) FACT_SALES (wm = sd.ingestion_ts)  -- MID-LEVEL KEY: (order_number, line_number)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'gold_dw.fact_sales'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'GOLD', 'gold_dw.fact_sales', 'STARTED', NOW(6), CONCAT('watermark=', @wm));

/* Requirements:
   - gold_dw.fact_sales has UNIQUE(order_number, line_number)
   - silver_dw.crm_sales_details provides a stable line_number (or equivalent)
*/
INSERT INTO gold_dw.fact_sales
(order_number, line_number, product_key, customer_key, order_date, shipping_date, due_date, sales_amount, quantity, price, src_ingestion_ts)
SELECT
  sd.sls_ord_num,
  sd.line_number,
  pr.product_key,
  cu.customer_key,
  sd.sls_order_dt,
  sd.sls_ship_dt,
  sd.sls_due_dt,
  sd.sls_sales,
  sd.sls_quantity,
  sd.sls_price,
  sd.ingestion_ts
FROM silver_dw.crm_sales_details sd
LEFT JOIN gold_dw.dim_products  pr ON sd.sls_prd_key  = pr.product_number
LEFT JOIN gold_dw.dim_customers cu ON sd.sls_cust_id  = cu.customer_id
WHERE sd.ingestion_ts IS NOT NULL
  AND sd.ingestion_ts > @wm
  AND sd.sls_ord_num IS NOT NULL
  AND sd.line_number IS NOT NULL
ON DUPLICATE KEY UPDATE
  product_key      = VALUES(product_key),
  customer_key     = VALUES(customer_key),
  order_date       = VALUES(order_date),
  shipping_date    = VALUES(shipping_date),
  due_date         = VALUES(due_date),
  sales_amount     = VALUES(sales_amount),
  quantity         = VALUES(quantity),
  price            = VALUES(price),
  src_ingestion_ts = VALUES(src_ingestion_ts);

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(sd.ingestion_ts) FROM silver_dw.crm_sales_details sd WHERE sd.ingestion_ts IS NOT NULL AND sd.ingestion_ts > @wm),
  @wm
)
WHERE table_name = 'gold_dw.fact_sales';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'GOLD',
  'gold_dw.fact_sales',
  'order_number_and_line_number_not_null',
  CASE WHEN SUM(CASE WHEN sd.sls_ord_num IS NULL OR sd.line_number IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN sd.sls_ord_num IS NULL OR sd.line_number IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM silver_dw.crm_sales_details sd
WHERE sd.ingestion_ts IS NOT NULL
  AND sd.ingestion_ts > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'GOLD', 'gold_dw.fact_sales', 'FINISHED', NOW(6), @rows, 'upsert completed');

SELECT 'GOLD LOAD FINISHED' AS status, @run_id AS run_id, @run_ts AS run_ts;