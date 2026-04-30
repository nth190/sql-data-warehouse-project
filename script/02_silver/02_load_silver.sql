/* =============================================================================
   SILVER LAYER - INCREMENTAL LOAD (Watermark + Upsert + Logging)
   Purpose:
     - Clean and standardize Bronze data into Silver tables
     - Incremental loads using control_dw.ctrl_watermark
     - Upsert using PRIMARY KEY / UNIQUE KEY on Silver tables
     - Write pipeline + DQ logs into control_dw

   Watermark strategy:
     - CRM tables: business date
         * silver_dw.crm_cust_info      -> cst_create_date
         * silver_dw.crm_prd_info       -> prd_start_dt
         * silver_dw.crm_sales_details  -> sls_order_dt
     - ERP tables: technical timestamp from Bronze
         * silver_dw.erp_cust_az12      -> ingestion_ts
         * silver_dw.erp_loc_a101       -> ingestion_ts
         * silver_dw.erp_px_cat_g1v2    -> ingestion_ts

   Requirements:
     - MySQL 8.0+ (uses window functions)
     - Silver tables must have proper PRIMARY KEY / UNIQUE constraints
============================================================================= */

-- Optional, for clarity (all targets are in silver_dw)
USE silver_dw;

-- -----------------------------------------------------------------------------
-- 0) Runtime parameters (one run_id for the whole SILVER run)
-- -----------------------------------------------------------------------------
SET @run_ts := NOW(6);
SET @run_id := DATE_FORMAT(@run_ts, '%Y%m%d%H%i%s%f');



/* =============================================================================
   A) SILVER - CRM_CUST_INFO (wm = cst_create_date)
   Grain:
     - 1 row = 1 customer (cst_id)
   Incremental logic:
     - Read new/changed rows from Bronze where cst_create_date > watermark
     - Keep the latest version per cst_id (ROW_NUMBER() window)
     - Upsert into Silver based on PRIMARY KEY (cst_id)
============================================================================= */

-- A1) Read watermark for this table
SET @wm_cust := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.crm_cust_info'
);

-- A2) Pipeline log - START
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, started_at, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.crm_cust_info',
  'STARTED',
  @run_ts,
  CONCAT('watermark=', @wm_cust)
);

-- A3) Incremental upsert from Bronze -> Silver
INSERT INTO silver_dw.crm_cust_info (
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date,
  ingestion_ts
)
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname)  AS cst_lastname,
  CASE
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'n/a'
  END AS cst_marital_status,
  CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'n/a'
  END AS cst_gndr,
  cst_create_date,
  @run_ts          AS ingestion_ts
FROM (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.cst_id
      ORDER BY b.cst_create_date DESC
    ) AS rn
  FROM bronze_dw.crm_cust_info b
  WHERE b.cst_create_date IS NOT NULL
    AND b.cst_create_date > @wm_cust
) x
WHERE x.rn = 1
  AND x.cst_id IS NOT NULL
  AND x.cst_id <> 0
ON DUPLICATE KEY UPDATE
  cst_key            = VALUES(cst_key),
  cst_firstname      = VALUES(cst_firstname),
  cst_lastname       = VALUES(cst_lastname),
  cst_marital_status = VALUES(cst_marital_status),
  cst_gndr           = VALUES(cst_gndr),
  cst_create_date    = VALUES(cst_create_date),
  ingestion_ts       = VALUES(ingestion_ts);

SET @rows_cust := ROW_COUNT();

-- A4) Update watermark (ANTI-NULL)
UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(b.cst_create_date)
    FROM bronze_dw.crm_cust_info b
    WHERE b.cst_create_date IS NOT NULL
      AND b.cst_create_date > @wm_cust
  ),
  @wm_cust
)
WHERE table_name = 'silver_dw.crm_cust_info';

-- A5) DQ log: check customer_id not null on this batch in SILVER
INSERT INTO control_dw.ctrl_dq_log (
  run_id, layer_name, table_name,
  dq_rule_name, dq_status,
  error_record_count, total_row_count, execution_ts
)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.crm_cust_info',
  'cst_id_not_null',
  CASE
    WHEN SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS dq_status,
  SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) AS error_record_count,
  COUNT(*) AS total_row_count,
  NOW(6)  AS execution_ts
FROM silver_dw.crm_cust_info
WHERE ingestion_ts = @run_ts;

-- A6) Pipeline log - END
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, ended_at, row_count, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.crm_cust_info',
  'FINISHED',
  NOW(6),
  @rows_cust,
  'upsert completed'
);



/* =============================================================================
   B) SILVER - CRM_PRD_INFO (wm = prd_start_dt)
   Grain:
     - 1 row = 1 product version (prd_id)
   Incremental logic:
     - New/updated products where prd_start_dt > watermark
     - Keep latest version per prd_id
============================================================================= */

-- B1) Read watermark
SET @wm_prd := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.crm_prd_info'
);

-- B2) Pipeline log - START
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, started_at, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.crm_prd_info',
  'STARTED',
  NOW(6),
  CONCAT('watermark=', @wm_prd)
);

-- B3) Incremental upsert
INSERT INTO silver_dw.crm_prd_info (
  prd_id,
  cat_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt,
  ingestion_ts
)
SELECT
  prd_id,
  cat_id,
  TRIM(prd_key) AS prd_key,
  TRIM(prd_nm)  AS prd_nm,
  prd_cost,
  TRIM(prd_line) AS prd_line,
  prd_start_dt,
  prd_end_dt,
  @run_ts        AS ingestion_ts
FROM (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.prd_id
      ORDER BY b.prd_start_dt DESC
    ) AS rn
  FROM bronze_dw.crm_prd_info b
  WHERE b.prd_start_dt IS NOT NULL
    AND b.prd_start_dt > @wm_prd
) x
WHERE x.rn = 1
  AND x.prd_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  cat_id       = VALUES(cat_id),
  prd_key      = VALUES(prd_key),
  prd_nm       = VALUES(prd_nm),
  prd_cost     = VALUES(prd_cost),
  prd_line     = VALUES(prd_line),
  prd_start_dt = VALUES(prd_start_dt),
  prd_end_dt   = VALUES(prd_end_dt),
  ingestion_ts = VALUES(ingestion_ts);

SET @rows_prd := ROW_COUNT();

-- B4) Update watermark
UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(b.prd_start_dt)
    FROM bronze_dw.crm_prd_info b
    WHERE b.prd_start_dt IS NOT NULL
      AND b.prd_start_dt > @wm_prd
  ),
  @wm_prd
)
WHERE table_name = 'silver_dw.crm_prd_info';

-- B5) DQ log: prd_id not null in this batch (Silver)
INSERT INTO control_dw.ctrl_dq_log (
  run_id, layer_name, table_name,
  dq_rule_name, dq_status,
  error_record_count, total_row_count, execution_ts
)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.crm_prd_info',
  'prd_id_not_null',
  CASE
    WHEN SUM(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS dq_status,
  SUM(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) AS error_record_count,
  COUNT(*) AS total_row_count,
  NOW(6)  AS execution_ts
FROM silver_dw.crm_prd_info
WHERE ingestion_ts = @run_ts;

-- B6) Pipeline log - END
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, ended_at, row_count, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.crm_prd_info',
  'FINISHED',
  NOW(6),
  @rows_prd,
  'upsert completed'
);



/* =============================================================================
   C) SILVER - CRM_SALES_DETAILS (wm = sls_order_dt)
   Grain:
     - 1 row = 1 order line (sls_ord_num, sls_prd_key)
   Incremental logic:
     - Business date = sls_order_dt
============================================================================= */

-- C1) Read watermark
SET @wm_sales := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.crm_sales_details'
);

-- C2) Pipeline log - START
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, started_at, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.crm_sales_details',
  'STARTED',
  NOW(6),
  CONCAT('watermark=', @wm_sales)
);

-- C3) Incremental upsert
INSERT INTO silver_dw.crm_sales_details (
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price,
  ingestion_ts
)
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price,
  @run_ts     AS ingestion_ts
FROM (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.sls_ord_num, b.sls_prd_key
      ORDER BY b.sls_order_dt DESC
    ) AS rn
  FROM bronze_dw.crm_sales_details b
  WHERE b.sls_order_dt IS NOT NULL
    AND b.sls_order_dt > @wm_sales
) x
WHERE x.rn = 1
  AND x.sls_ord_num IS NOT NULL
  AND x.sls_prd_key IS NOT NULL
ON DUPLICATE KEY UPDATE
  sls_cust_id  = VALUES(sls_cust_id),
  sls_order_dt = VALUES(sls_order_dt),
  sls_ship_dt  = VALUES(sls_ship_dt),
  sls_due_dt   = VALUES(sls_due_dt),
  sls_sales    = VALUES(sls_sales),
  sls_quantity = VALUES(sls_quantity),
  sls_price    = VALUES(sls_price),
  ingestion_ts = VALUES(ingestion_ts);

SET @rows_sales := ROW_COUNT();

-- C4) Update watermark
UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(b.sls_order_dt)
    FROM bronze_dw.crm_sales_details b
    WHERE b.sls_order_dt IS NOT NULL
      AND b.sls_order_dt > @wm_sales
  ),
  @wm_sales
)
WHERE table_name = 'silver_dw.crm_sales_details';

-- C5) DQ log: order_number not null (Silver)
INSERT INTO control_dw.ctrl_dq_log (
  run_id, layer_name, table_name,
  dq_rule_name, dq_status,
  error_record_count, total_row_count, execution_ts
)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.crm_sales_details',
  'sls_ord_num_not_null',
  CASE
    WHEN SUM(CASE WHEN sls_ord_num IS NULL THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS dq_status,
  SUM(CASE WHEN sls_ord_num IS NULL THEN 1 ELSE 0 END) AS error_record_count,
  COUNT(*) AS total_row_count,
  NOW(6)  AS execution_ts
FROM silver_dw.crm_sales_details
WHERE ingestion_ts = @run_ts;

-- C6) Pipeline log - END
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, ended_at, row_count, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.crm_sales_details',
  'FINISHED',
  NOW(6),
  @rows_sales,
  'upsert completed'
);



/* =============================================================================
   D) SILVER - ERP_CUST_AZ12 (wm = ingestion_ts)
   Grain:
     - 1 row = 1 ERP customer (cid)
   Incremental logic:
     - ingestion_ts from Bronze is the technical watermark
============================================================================= */

-- D1) Read watermark
SET @wm_az12 := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.erp_cust_az12'
);

-- D2) Pipeline log - START
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, started_at, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.erp_cust_az12',
  'STARTED',
  NOW(6),
  CONCAT('watermark=', @wm_az12)
);

-- D3) Incremental upsert
INSERT INTO silver_dw.erp_cust_az12 (
  cid,
  bdate,
  gen,
  ingestion_ts
)
SELECT
  b.cid,
  b.bdate,
  b.gen,
  @run_ts
FROM bronze_dw.erp_cust_az12 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm_az12
  AND b.cid IS NOT NULL
ON DUPLICATE KEY UPDATE
  bdate        = VALUES(bdate),
  gen          = VALUES(gen),
  ingestion_ts = VALUES(ingestion_ts);

SET @rows_az12 := ROW_COUNT();

-- D4) Update watermark
UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(b.ingestion_ts)
    FROM bronze_dw.erp_cust_az12 b
    WHERE b.ingestion_ts IS NOT NULL
      AND b.ingestion_ts > @wm_az12
  ),
  @wm_az12
)
WHERE table_name = 'silver_dw.erp_cust_az12';

-- D5) DQ log: cid not null (Silver)
INSERT INTO control_dw.ctrl_dq_log (
  run_id, layer_name, table_name,
  dq_rule_name, dq_status,
  error_record_count, total_row_count, execution_ts
)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.erp_cust_az12',
  'cid_not_null',
  CASE
    WHEN SUM(CASE WHEN cid IS NULL THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS dq_status,
  SUM(CASE WHEN cid IS NULL THEN 1 ELSE 0 END) AS error_record_count,
  COUNT(*) AS total_row_count,
  NOW(6)  AS execution_ts
FROM silver_dw.erp_cust_az12
WHERE ingestion_ts = @run_ts;

-- D6) Pipeline log - END
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, ended_at, row_count, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.erp_cust_az12',
  'FINISHED',
  NOW(6),
  @rows_az12,
  'upsert completed'
);



/* =============================================================================
   E) SILVER - ERP_LOC_A101 (wm = ingestion_ts)
   Grain:
     - 1 row = 1 customer location (cid)
============================================================================= */

-- E1) Read watermark
SET @wm_loc := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.erp_loc_a101'
);

-- E2) Pipeline log - START
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, started_at, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.erp_loc_a101',
  'STARTED',
  NOW(6),
  CONCAT('watermark=', @wm_loc)
);

-- E3) Incremental upsert
INSERT INTO silver_dw.erp_loc_a101 (
  cid,
  cntry,
  ingestion_ts
)
SELECT
  b.cid,
  TRIM(b.cntry) AS cntry,
  @run_ts
FROM bronze_dw.erp_loc_a101 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm_loc
  AND b.cid IS NOT NULL
ON DUPLICATE KEY UPDATE
  cntry        = VALUES(cntry),
  ingestion_ts = VALUES(ingestion_ts);

SET @rows_loc := ROW_COUNT();

-- E4) Update watermark
UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(b.ingestion_ts)
    FROM bronze_dw.erp_loc_a101 b
    WHERE b.ingestion_ts IS NOT NULL
      AND b.ingestion_ts > @wm_loc
  ),
  @wm_loc
)
WHERE table_name = 'silver_dw.erp_loc_a101';

-- E5) DQ log: cid not null (Silver)
INSERT INTO control_dw.ctrl_dq_log (
  run_id, layer_name, table_name,
  dq_rule_name, dq_status,
  error_record_count, total_row_count, execution_ts
)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.erp_loc_a101',
  'cid_not_null',
  CASE
    WHEN SUM(CASE WHEN cid IS NULL THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS dq_status,
  SUM(CASE WHEN cid IS NULL THEN 1 ELSE 0 END) AS error_record_count,
  COUNT(*) AS total_row_count,
  NOW(6)  AS execution_ts
FROM silver_dw.erp_loc_a101
WHERE ingestion_ts = @run_ts;

-- E6) Pipeline log - END
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, ended_at, row_count, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.erp_loc_a101',
  'FINISHED',
  NOW(6),
  @rows_loc,
  'upsert completed'
);



/* =============================================================================
   F) SILVER - ERP_PX_CAT_G1V2 (wm = ingestion_ts)
   Grain:
     - 1 row = 1 category mapping (id)
============================================================================= */

-- F1) Read watermark
SET @wm_cat := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.erp_px_cat_g1v2'
);

-- F2) Pipeline log - START
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, started_at, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.erp_px_cat_g1v2',
  'STARTED',
  NOW(6),
  CONCAT('watermark=', @wm_cat)
);

-- F3) Incremental upsert
INSERT INTO silver_dw.erp_px_cat_g1v2 (
  id,
  cat,
  subcat,
  maintenance,
  ingestion_ts
)
SELECT
  b.id,
  TRIM(b.cat)         AS cat,
  TRIM(b.subcat)      AS subcat,
  TRIM(b.maintenance) AS maintenance,
  @run_ts
FROM bronze_dw.erp_px_cat_g1v2 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm_cat
  AND b.id IS NOT NULL
ON DUPLICATE KEY UPDATE
  cat         = VALUES(cat),
  subcat      = VALUES(subcat),
  maintenance = VALUES(maintenance),
  ingestion_ts= VALUES(ingestion_ts);

SET @rows_cat := ROW_COUNT();

-- F4) Update watermark
UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (
    SELECT MAX(b.ingestion_ts)
    FROM bronze_dw.erp_px_cat_g1v2 b
    WHERE b.ingestion_ts IS NOT NULL
      AND b.ingestion_ts > @wm_cat
  ),
  @wm_cat
)
WHERE table_name = 'silver_dw.erp_px_cat_g1v2';

-- F5) DQ log: id not null (Silver)
INSERT INTO control_dw.ctrl_dq_log (
  run_id, layer_name, table_name,
  dq_rule_name, dq_status,
  error_record_count, total_row_count, execution_ts
)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.erp_px_cat_g1v2',
  'id_not_null',
  CASE
    WHEN SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) = 0
      THEN 'PASS'
    ELSE 'FAIL'
  END AS dq_status,
  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS error_record_count,
  COUNT(*) AS total_row_count,
  NOW(6)  AS execution_ts
FROM silver_dw.erp_px_cat_g1v2
WHERE ingestion_ts = @run_ts;

-- F6) Pipeline log - END
INSERT INTO control_dw.ctrl_pipeline_log (
  run_id, layer_name, object_name, status, ended_at, row_count, message
)
VALUES (
  @run_id,
  'SILVER',
  'silver_dw.erp_px_cat_g1v2',
  'FINISHED',
  NOW(6),
  @rows_cat,
  'upsert completed'
);



-- =============================================================================
-- FINAL STATUS
-- =============================================================================
SELECT 'SILVER LOAD FINISHED' AS status,
       @run_id AS run_id,
       @run_ts AS run_ts;
       