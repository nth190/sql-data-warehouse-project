/* =============================================================================
   GOLD LAYER - INCREMENTAL LOAD (Watermark + Upsert + Logging)
   Targets:
     - gold_dw.dim_customers  (wm = ci.cst_create_date)
     - gold_dw.dim_products   (wm = pn.prd_start_dt)
     - gold_dw.fact_sales     (wm = sd.ingestion_ts)

   Control tables (created in control_dw.ddl_control.sql):
     - control_dw.ctrl_watermark
     - control_dw.ctrl_pipeline_log
     - control_dw.ctrl_dq_log
============================================================================= */

-- Optional: not required, but helps to ensure we are in the right schema
USE gold_dw;

-- ---------------------------------------------------------------------------
-- 0) Global run context (one run_id for the entire GOLD load)
-- ---------------------------------------------------------------------------
SET @run_ts := NOW(6);
SET @run_id := DATE_FORMAT(@run_ts, '%Y%m%d%H%i%s%f');



/* ============================================================================
   GOLD LAYER - dim_customers
   Incremental SCD Type 2 Implementation

   Business Rules:
     - New customer → insert as current record
     - Changed customer → 
         1) close existing current record
         2) insert new current version

   Watermark strategy:
     - Use change timestamp (last_updated_ts / ingestion_ts)
     - Process only records > watermark
============================================================================ */

USE gold_dw;

-- ============================================================================
-- 1. Read watermark
-- ============================================================================

SET @wm_dim_customers := (
    SELECT COALESCE(last_ts, '1900-01-01')
    FROM control_dw.ctrl_watermark
    WHERE table_name = 'gold_dw.dim_customers'
);

-- ============================================================================
-- 2. Write START log
-- ============================================================================

INSERT INTO control_dw.ctrl_pipeline_log (
    run_id,
    layer_name,
    object_name,
    status,
    started_at,
    message
)
VALUES (
    @run_id,
    'GOLD',
    'gold_dw.dim_customers',
    'STARTED',
    @run_ts,
    CONCAT('watermark=', @wm_dim_customers)
);

-- ============================================================================
-- 3. Collect changed customers since watermark
--    change_ts drives:
--       - SCD2 version start
--       - watermark update
-- ============================================================================

DROP TEMPORARY TABLE IF EXISTS tmp_dim_customers_changes;

CREATE TEMPORARY TABLE tmp_dim_customers_changes AS
SELECT
    ci.cst_id              AS customer_id,
    ci.cst_key             AS customer_number,
    ci.cst_firstname       AS first_name,
    ci.cst_lastname        AS last_name,
    la.cntry               AS country,
    ci.cst_marital_status  AS marital_status,

    CASE
        WHEN ci.cst_gndr IS NULL THEN 'n/a'
        WHEN ca.gen IS NULL OR ca.gen = 'n/a' THEN ci.cst_gndr
        WHEN ci.cst_gndr <> ca.gen THEN ca.gen
        ELSE COALESCE(ca.gen, 'n/a')
    END                     AS gender,

    ca.bdate                AS birthdate,
    ci.cst_create_date      AS create_date,

    COALESCE(
        ci.last_updated_ts,
        ci.ingestion_ts
    )                       AS change_ts,

    ci.ingestion_ts         AS src_ingestion_ts

FROM silver_dw.crm_cust_info ci
LEFT JOIN silver_dw.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver_dw.erp_loc_a101 la
    ON ci.cst_key = la.cid
WHERE
    COALESCE(ci.last_updated_ts, ci.ingestion_ts) > @wm_dim_customers;

-- ============================================================================
-- 4. Close existing current records (SCD2 step 1)
-- ============================================================================

UPDATE gold_dw.dim_customers g
JOIN tmp_dim_customers_changes c
    ON g.customer_id = c.customer_id
   AND g.is_current  = 1
SET
    g.effective_end_date = GREATEST(
        DATE(c.change_ts) - INTERVAL 1 DAY,
        g.effective_start_date
    ),
    g.is_current = 0;

-- ============================================================================
-- 5. Insert new current version (SCD2 step 2)
-- ============================================================================

INSERT INTO gold_dw.dim_customers (
    customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date,
    effective_start_date,
    effective_end_date,
    is_current,
    src_ingestion_ts
)
SELECT
    c.customer_id,
    c.customer_number,
    c.first_name,
    c.last_name,
    c.country,
    c.marital_status,
    c.gender,
    c.birthdate,
    c.create_date,
    DATE(c.change_ts)      AS effective_start_date,
    '9999-12-31'           AS effective_end_date,
    1                      AS is_current,
    c.src_ingestion_ts
FROM tmp_dim_customers_changes c;

SET @rows_dim_customers := ROW_COUNT();

-- ============================================================================
-- 6. Update watermark
-- ============================================================================

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
    (SELECT MAX(change_ts) FROM tmp_dim_customers_changes),
    @wm_dim_customers
)
WHERE table_name = 'gold_dw.dim_customers';

-- ============================================================================
-- 7. Data quality check
--    Rule: customer_id must not be NULL
-- ============================================================================

INSERT INTO control_dw.ctrl_dq_log (
    run_id,
    layer_name,
    table_name,
    dq_rule_name,
    dq_status,
    error_record_count,
    total_row_count,
    execution_ts
)
SELECT
    @run_id,
    'GOLD',
    'gold_dw.dim_customers',
    'customer_id_not_null',
    CASE
        WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0
            THEN 'PASS'
        ELSE 'FAIL'
    END,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    NOW(6)
FROM tmp_dim_customers_changes;

-- ============================================================================
-- 8. Write FINISHED log
-- ============================================================================

INSERT INTO control_dw.ctrl_pipeline_log (
    run_id,
    layer_name,
    object_name,
    status,
    started_at,
    ended_at,
    row_count,
    message
)
VALUES (
    @run_id,
    'GOLD',
    'gold_dw.dim_customers',
    'FINISHED',
    @run_ts,
    NOW(6),
    @rows_dim_customers,
    'SCD2 incremental completed'
);




/* ============================================================================
   B) DIM_PRODUCTS (SCD Type 2, watermark = pn.prd_start_dt)
   ----------------------------------------------------------------------------
   - Source tables:
       silver_dw.crm_prd_info pn
       silver_dw.erp_px_cat_g1v2 pc
   - Grain:
       1 row = 1 version of a product (current or historical)
   - SCD2 logic:
       * Only consider rows where pn.prd_start_dt > watermark
       * Detect changed attributes vs current version (is_current = 1)
       * Close old row (set effective_end_date, is_current = 0)
       * Insert new row with new attributes and effective_start_date
============================================================================= */

-- B1) Read watermark for dim_products
SET @wm_dim_products := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'gold_dw.dim_products'
);

-- B2) Write START log for dim_products
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status,
  started_at, ended_at, row_count, message
)
VALUES (
  @run_id,
  'GOLD',
  'gold_dw.dim_products',
  'STARTED',
  @run_ts,
  NULL,
  NULL,
  CONCAT('watermark=', @wm_dim_products)
);

-- ---------------------------------------------------------------------------
-- B3) Build a temporary snapshot of candidate product records
--      (only new or potentially changed rows after the watermark)
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_dim_products_changes;

CREATE TEMPORARY TABLE tmp_dim_products_changes AS
SELECT
  pn.prd_id        AS product_id,
  pn.prd_key       AS product_number,
  pn.prd_nm        AS product_name,
  pn.cat_id        AS category_id,
  pc.cat           AS category,
  pc.subcat        AS subcategory,
  pc.maintenance   AS maintenance,
  pn.prd_cost      AS cost,
  pn.prd_line      AS product_line,
  pn.prd_start_dt  AS start_date,
  pn.prd_start_dt  AS effective_start_date,
  pn.ingestion_ts  AS src_ingestion_ts
FROM silver_dw.crm_prd_info pn
LEFT JOIN silver_dw.erp_px_cat_g1v2 pc
  ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL
  AND pn.prd_start_dt IS NOT NULL
  AND pn.prd_start_dt > @wm_dim_products;

-- Optional: quick check
-- SELECT COUNT(*) AS candidate_rows FROM tmp_dim_products_changes;

-- ---------------------------------------------------------------------------
-- B4) Close old versions for changed products
--      (compare with current version using NULL-safe equality <=>)
-- ---------------------------------------------------------------------------

-- Only consider CURRENT rows in dim_products and join with candidate snapshot
UPDATE gold_dw.dim_products dp
JOIN tmp_dim_products_changes src
  ON dp.product_number = src.product_number
 AND dp.is_current = 1
SET
  dp.effective_end_date = DATE_SUB(src.effective_start_date, INTERVAL 1 DAY),
  dp.is_current         = 0
WHERE NOT (
  dp.product_name  <=> src.product_name  AND
  dp.category_id   <=> src.category_id   AND
  dp.category      <=> src.category      AND
  dp.subcategory   <=> src.subcategory   AND
  dp.maintenance   <=> src.maintenance   AND
  dp.cost          <=> src.cost          AND
  dp.product_line  <=> src.product_line  AND
  dp.start_date    <=> src.start_date
);

-- ---------------------------------------------------------------------------
-- B5) Insert new versions (for NEW products and CHANGED products)
--      - New product_number: no current row exists
--      - Changed product_number: old row has just been closed above
-- ---------------------------------------------------------------------------

INSERT INTO gold_dw.dim_products (
  product_id,
  product_number,
  product_name,
  category_id,
  category,
  subcategory,
  maintenance,
  cost,
  product_line,
  start_date,
  effective_start_date,
  effective_end_date,
  is_current,
  src_ingestion_ts
)
SELECT
  src.product_id,
  src.product_number,
  src.product_name,
  src.category_id,
  src.category,
  src.subcategory,
  src.maintenance,
  src.cost,
  src.product_line,
  src.start_date,
  src.effective_start_date,
  '9999-12-31' AS effective_end_date,
  1            AS is_current,
  src.src_ingestion_ts
FROM tmp_dim_products_changes src
LEFT JOIN gold_dw.dim_products dp_current
  ON dp_current.product_number = src.product_number
 AND dp_current.is_current = 1
WHERE
  -- Insert if there is no current row at all (new product)
  dp_current.product_key IS NULL

  OR

  -- Or insert if any attribute is different from current row
  NOT (
    dp_current.product_name  <=> src.product_name  AND
    dp_current.category_id   <=> src.category_id   AND
    dp_current.category      <=> src.category      AND
    dp_current.subcategory   <=> src.subcategory   AND
    dp_current.maintenance   <=> src.maintenance   AND
    dp_current.cost          <=> src.cost          AND
    dp_current.product_line  <=> src.product_line  AND
    dp_current.start_date    <=> src.start_date
  );

SET @rows_dim_products := ROW_COUNT();

-- ---------------------------------------------------------------------------
-- B6) Update watermark for dim_products
-- ---------------------------------------------------------------------------

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(pn.prd_start_dt)
    FROM silver_dw.crm_prd_info pn
    WHERE pn.prd_end_dt IS NULL
      AND pn.prd_start_dt IS NOT NULL
      AND pn.prd_start_dt > @wm_dim_products
  ),
  @wm_dim_products
)
WHERE table_name = 'gold_dw.dim_products';

-- ---------------------------------------------------------------------------
-- B7) Simple Data Quality rule: product_number must not be NULL
-- ---------------------------------------------------------------------------

INSERT INTO control_dw.ctrl_dq_log (
  run_id,
  layer_name,
  table_name,
  dq_rule_name,
  dq_status,
  error_record_count,
  total_row_count,
  execution_ts
)
SELECT
  @run_id,
  'GOLD',
  'gold_dw.dim_products',
  'product_number_not_null',
  CASE
    WHEN SUM(CASE WHEN pn.prd_key IS NULL THEN 1 ELSE 0 END) = 0
    THEN 'PASS' ELSE 'FAIL'
  END AS dq_status,
  SUM(CASE WHEN pn.prd_key IS NULL THEN 1 ELSE 0 END) AS error_record_count,
  COUNT(*) AS total_row_count,
  NOW(6) AS execution_ts
FROM silver_dw.crm_prd_info pn
WHERE pn.prd_end_dt IS NULL
  AND pn.prd_start_dt IS NOT NULL
  AND pn.prd_start_dt > @wm_dim_products;

-- ---------------------------------------------------------------------------
-- B8) Write FINISHED log for dim_products
-- ---------------------------------------------------------------------------

INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status,
  started_at, ended_at, row_count, message
)
VALUES (
  @run_id,
  'GOLD',
  'gold_dw.dim_products',
  'FINISHED',
  @run_ts,
  NOW(6),
  @rows_dim_products,
  'SCD Type 2 upsert completed'
);


/* ============================================================================
   C) FACT_SALES (incremental + SCD2 lookup)
   ---------------------------------------------------------------------------
   - Source:
       silver_dw.crm_sales_details      (sd)
       gold_dw.dim_products  (SCD2)     (pr)
       gold_dw.dim_customers (SCD2)     (cu)

   - Grain:
       1 row = 1 order_number + 1 product_key + 1 customer_key

   - SCD2 logic for dimension lookups:
       - Join customer using:
           sd.sls_cust_id = cu.customer_id
           AND sd.sls_order_dt BETWEEN cu.effective_start_date
                                  AND cu.effective_end_date - 1 day
       - Join product using:
           sd.sls_prd_key = pr.product_number
           AND sd.sls_order_dt BETWEEN pr.effective_start_date
                                  AND pr.effective_end_date - 1 day

   - Incremental logic:
       - Use ctrl_watermark.table_name = 'gold_dw.fact_sales'
       - Watermark column: sd.ingestion_ts
       - Only process rows with sd.ingestion_ts > @wm_fact_sales
============================================================================= */

-- C1) Read watermark for fact_sales
SET @wm_fact_sales := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'gold_dw.fact_sales'
);

-- C2) Pipeline log - START
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id,
  layer_name,
  object_name,
  status,
  started_at,
  ended_at,
  row_count,
  message
)
VALUES (
  @run_id,
  'GOLD',
  'gold_dw.fact_sales',
  'STARTED',
  @run_ts,
  NULL,
  NULL,
  CONCAT('watermark=', @wm_fact_sales)
);

-- C3) Incremental upsert into fact_sales
INSERT INTO gold_dw.fact_sales (
  order_number,
  product_key,
  customer_key,
  order_date,
  shipping_date,
  due_date,
  sales_amount,
  quantity,
  price,
  src_ingestion_ts
)
SELECT
  sd.sls_ord_num          AS order_number,

  -- SCD2 product version (correct version at order_date)
  pr.product_key          AS product_key,

  -- SCD2 customer version (correct version at order_date)
  cu.customer_key         AS customer_key,

  sd.sls_order_dt         AS order_date,
  sd.sls_ship_dt          AS shipping_date,
  sd.sls_due_dt           AS due_date,
  sd.sls_sales            AS sales_amount,
  sd.sls_quantity         AS quantity,
  sd.sls_price            AS price,
  sd.ingestion_ts         AS src_ingestion_ts
FROM silver_dw.crm_sales_details sd
JOIN gold_dw.dim_products pr
  ON sd.sls_prd_key = pr.product_number
  AND sd.sls_order_dt >= pr.effective_start_date
  AND sd.sls_order_dt <  pr.effective_end_date
JOIN gold_dw.dim_customers cu
  ON sd.sls_cust_id = cu.customer_id
  AND sd.sls_order_dt >= cu.effective_start_date
  AND sd.sls_order_dt <  cu.effective_end_date
WHERE sd.ingestion_ts IS NOT NULL
  AND sd.ingestion_ts > @wm_fact_sales
  AND sd.sls_ord_num IS NOT NULL
  AND sd.sls_prd_key IS NOT NULL
  AND sd.sls_cust_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  order_date       = VALUES(order_date),
  shipping_date    = VALUES(shipping_date),
  due_date         = VALUES(due_date),
  sales_amount     = VALUES(sales_amount),
  quantity         = VALUES(quantity),
  price            = VALUES(price),
  src_ingestion_ts = VALUES(src_ingestion_ts);

SET @rows_fact_sales := ROW_COUNT();

-- C4) Update watermark for fact_sales
UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(sd2.ingestion_ts)
    FROM silver_dw.crm_sales_details sd2
    WHERE sd2.ingestion_ts IS NOT NULL
      AND sd2.ingestion_ts > @wm_fact_sales
  ),
  @wm_fact_sales
)
WHERE table_name = 'gold_dw.fact_sales';

-- C5) DQ log: order_number + product_key + customer_key must not be NULL
INSERT INTO control_dw.ctrl_dq_log (
  run_id,
  layer_name,
  table_name,
  dq_rule_name,
  dq_status,
  error_record_count,
  total_row_count,
  execution_ts
)
SELECT
  @run_id                                        AS run_id,
  'GOLD'                                         AS layer_name,
  'gold_dw.fact_sales'                           AS table_name,
  'order_product_customer_not_null'              AS dq_rule_name,
  CASE
    WHEN SUM(
           CASE
             WHEN sd.sls_ord_num IS NULL
               OR pr.product_key IS NULL
               OR cu.customer_key IS NULL
             THEN 1 ELSE 0
           END
         ) = 0
    THEN 'PASS'
    ELSE 'FAIL'
  END                                            AS dq_status,
  SUM(
    CASE
      WHEN sd.sls_ord_num IS NULL
        OR pr.product_key IS NULL
        OR cu.customer_key IS NULL
      THEN 1 ELSE 0
    END
  )                                              AS error_record_count,
  COUNT(*)                                       AS total_row_count,
  NOW(6)                                         AS execution_ts
FROM silver_dw.crm_sales_details sd
JOIN gold_dw.dim_products pr
  ON sd.sls_prd_key = pr.product_number
  AND sd.sls_order_dt >= pr.effective_start_date
  AND sd.sls_order_dt <  pr.effective_end_date
JOIN gold_dw.dim_customers cu
  ON sd.sls_cust_id = cu.customer_id
  AND sd.sls_order_dt >= cu.effective_start_date
  AND sd.sls_order_dt <  cu.effective_end_date
WHERE sd.ingestion_ts IS NOT NULL
  AND sd.ingestion_ts > @wm_fact_sales;

-- C6) Pipeline log - FINISHED
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id,
  layer_name,
  object_name,
  status,
  started_at,
  ended_at,
  row_count,
  message
)
VALUES (
  @run_id,
  'GOLD',
  'gold_dw.fact_sales',
  'FINISHED',
  @run_ts,
  NOW(6),
  @rows_fact_sales,
  'Incremental upsert with SCD2 lookups completed'
);

