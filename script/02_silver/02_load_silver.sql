/* =============================================================================
   SILVER LAYER - INCREMENTAL LOAD (Watermark + Upsert + Logging)
   Purpose:
     - Clean + standardize Bronze data into Silver tables
     - Incremental loads using control_dw.ctrl_watermark
     - Upsert using UNIQUE/PK on target tables
     - Write pipeline log + dq log to control_dw

   Watermark strategy:
     - CRM tables: business date
         * crm_cust_info      -> cst_create_date
         * crm_prd_info       -> prd_start_dt
         * crm_sales_details  -> sls_order_dt
     - ERP tables: ingestion_ts (technical timestamp)

   Requirements:
     - MySQL 8.0+ (uses window functions)
     - Silver tables must have proper UNIQUE/PK for ON DUPLICATE KEY UPDATE
============================================================================= */

-- -----------------------------------------------------------------------------
-- 0) Runtime parameters (one run_id for the whole SILVER run)
-- -----------------------------------------------------------------------------
SET @run_ts := NOW(6);
SET @run_id := DATE_FORMAT(@run_ts, '%Y%m%d%H%i%s%f');

-- =============================================================================
-- A) silver_dw.crm_cust_info  (wm = cst_create_date)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.crm_cust_info'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.crm_cust_info', 'STARTED', @run_ts, CONCAT('watermark=', @wm));

INSERT INTO silver_dw.crm_cust_info
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date, ingestion_ts)
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname),
  TRIM(cst_lastname),
  CASE
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'n/a'
  END,
  CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'n/a'
  END,
  cst_create_date,
  @run_ts
FROM (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.cst_id ORDER BY b.cst_create_date DESC) AS rn
  FROM bronze_dw.crm_cust_info b
  WHERE b.cst_create_date IS NOT NULL
    AND b.cst_create_date > @wm
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

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(b.cst_create_date) FROM bronze_dw.crm_cust_info b WHERE b.cst_create_date IS NOT NULL AND b.cst_create_date > @wm),
  @wm
)
WHERE table_name = 'silver_dw.crm_cust_info';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.crm_cust_info',
  'cst_id_not_null',
  CASE WHEN SUM(CASE WHEN b.cst_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN b.cst_id IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM bronze_dw.crm_cust_info b
WHERE b.cst_create_date IS NOT NULL
  AND b.cst_create_date > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.crm_cust_info', 'FINISHED', NOW(6), @rows, 'upsert completed');

-- =============================================================================
-- B) silver_dw.crm_prd_info  (wm = prd_start_dt)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.crm_prd_info'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.crm_prd_info', 'STARTED', NOW(6), CONCAT('watermark=', @wm));

INSERT INTO silver_dw.crm_prd_info
(prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt, ingestion_ts)
SELECT
  prd_id,
  prd_key,
  TRIM(prd_nm),
  prd_cost,
  TRIM(prd_line),
  prd_start_dt,
  prd_end_dt,
  @run_ts
FROM (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.prd_id ORDER BY b.prd_start_dt DESC) AS rn
  FROM bronze_dw.crm_prd_info b
  WHERE b.prd_start_dt IS NOT NULL
    AND b.prd_start_dt > @wm
) x
WHERE x.rn = 1
  AND x.prd_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  prd_key      = VALUES(prd_key),
  prd_nm       = VALUES(prd_nm),
  prd_cost     = VALUES(prd_cost),
  prd_line     = VALUES(prd_line),
  prd_start_dt = VALUES(prd_start_dt),
  prd_end_dt   = VALUES(prd_end_dt),
  ingestion_ts = VALUES(ingestion_ts);

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(b.prd_start_dt) FROM bronze_dw.crm_prd_info b WHERE b.prd_start_dt IS NOT NULL AND b.prd_start_dt > @wm),
  @wm
)
WHERE table_name = 'silver_dw.crm_prd_info';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.crm_prd_info',
  'prd_id_not_null',
  CASE WHEN SUM(CASE WHEN b.prd_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN b.prd_id IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM bronze_dw.crm_prd_info b
WHERE b.prd_start_dt IS NOT NULL
  AND b.prd_start_dt > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.crm_prd_info', 'FINISHED', NOW(6), @rows, 'upsert completed');

-- =============================================================================
-- C) silver_dw.crm_sales_details  (wm = sls_order_dt)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.crm_sales_details'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.crm_sales_details', 'STARTED', NOW(6), CONCAT('watermark=', @wm));

/* NOTE:
   Requires UNIQUE KEY on silver target: (sls_ord_num, sls_prd_key) (you already added uk_sales)
*/
INSERT INTO silver_dw.crm_sales_details
(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price, ingestion_ts)
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
  @run_ts
FROM (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY b.sls_ord_num, b.sls_prd_key ORDER BY b.sls_order_dt DESC) AS rn
  FROM bronze_dw.crm_sales_details b
  WHERE b.sls_order_dt IS NOT NULL
    AND b.sls_order_dt > @wm
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

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(b.sls_order_dt) FROM bronze_dw.crm_sales_details b WHERE b.sls_order_dt IS NOT NULL AND b.sls_order_dt > @wm),
  @wm
)
WHERE table_name = 'silver_dw.crm_sales_details';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.crm_sales_details',
  'order_number_not_null',
  CASE WHEN SUM(CASE WHEN b.sls_ord_num IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN b.sls_ord_num IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM bronze_dw.crm_sales_details b
WHERE b.sls_order_dt IS NOT NULL
  AND b.sls_order_dt > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.crm_sales_details', 'FINISHED', NOW(6), @rows, 'upsert completed');

-- =============================================================================
-- D) silver_dw.erp_cust_az12  (wm = ingestion_ts)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.erp_cust_az12'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.erp_cust_az12', 'STARTED', NOW(6), CONCAT('watermark=', @wm));

INSERT INTO silver_dw.erp_cust_az12 (cid, bdate, gen, ingestion_ts)
SELECT
  b.cid,
  b.bdate,
  b.gen,
  @run_ts
FROM bronze_dw.erp_cust_az12 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm
  AND b.cid IS NOT NULL
ON DUPLICATE KEY UPDATE
  bdate        = VALUES(bdate),
  gen          = VALUES(gen),
  ingestion_ts = VALUES(ingestion_ts);

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(b.ingestion_ts) FROM bronze_dw.erp_cust_az12 b WHERE b.ingestion_ts IS NOT NULL AND b.ingestion_ts > @wm),
  @wm
)
WHERE table_name = 'silver_dw.erp_cust_az12';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.erp_cust_az12',
  'cid_not_null',
  CASE WHEN SUM(CASE WHEN b.cid IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN b.cid IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM bronze_dw.erp_cust_az12 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.erp_cust_az12', 'FINISHED', NOW(6), @rows, 'upsert completed');

-- =============================================================================
-- E) silver_dw.erp_loc_a101  (wm = ingestion_ts)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.erp_loc_a101'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.erp_loc_a101', 'STARTED', NOW(6), CONCAT('watermark=', @wm));

INSERT INTO silver_dw.erp_loc_a101 (cid, cntry, ingestion_ts)
SELECT
  b.cid,
  TRIM(b.cntry),
  @run_ts
FROM bronze_dw.erp_loc_a101 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm
  AND b.cid IS NOT NULL
ON DUPLICATE KEY UPDATE
  cntry        = VALUES(cntry),
  ingestion_ts = VALUES(ingestion_ts);

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(b.ingestion_ts) FROM bronze_dw.erp_loc_a101 b WHERE b.ingestion_ts IS NOT NULL AND b.ingestion_ts > @wm),
  @wm
)
WHERE table_name = 'silver_dw.erp_loc_a101';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.erp_loc_a101',
  'cid_not_null',
  CASE WHEN SUM(CASE WHEN b.cid IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN b.cid IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM bronze_dw.erp_loc_a101 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.erp_loc_a101', 'FINISHED', NOW(6), @rows, 'upsert completed');

-- =============================================================================
-- F) silver_dw.erp_px_cat_g1v2  (wm = ingestion_ts)
-- =============================================================================
SET @wm := (
  SELECT COALESCE(last_ts, '1900-01-01')
  FROM control_dw.ctrl_watermark
  WHERE table_name = 'silver_dw.erp_px_cat_g1v2'
);

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, started_at, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.erp_px_cat_g1v2', 'STARTED', NOW(6), CONCAT('watermark=', @wm));

/* NOTE:
   Requires UNIQUE/PK on silver target: (id)
*/
INSERT INTO silver_dw.erp_px_cat_g1v2 (id, cat, subcat, maintenance, ingestion_ts)
SELECT
  b.id,
  TRIM(b.cat),
  TRIM(b.subcat),
  TRIM(b.maintenance),
  @run_ts
FROM bronze_dw.erp_px_cat_g1v2 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm
  AND b.id IS NOT NULL
ON DUPLICATE KEY UPDATE
  cat         = VALUES(cat),
  subcat      = VALUES(subcat),
  maintenance = VALUES(maintenance),
  ingestion_ts= VALUES(ingestion_ts);

SET @rows := ROW_COUNT();

UPDATE control_dw.ctrl_watermark
SET last_ts = COALESCE(
  (SELECT MAX(b.ingestion_ts) FROM bronze_dw.erp_px_cat_g1v2 b WHERE b.ingestion_ts IS NOT NULL AND b.ingestion_ts > @wm),
  @wm
)
WHERE table_name = 'silver_dw.erp_px_cat_g1v2';

INSERT INTO control_dw.ctrl_dq_log
(run_id, layer_name, table_name, dq_rule_name, dq_status, error_record_count, total_row_count, execution_ts)
SELECT
  @run_id,
  'SILVER',
  'silver_dw.erp_px_cat_g1v2',
  'id_not_null',
  CASE WHEN SUM(CASE WHEN b.id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
  SUM(CASE WHEN b.id IS NULL THEN 1 ELSE 0 END),
  COUNT(*),
  NOW(6)
FROM bronze_dw.erp_px_cat_g1v2 b
WHERE b.ingestion_ts IS NOT NULL
  AND b.ingestion_ts > @wm;

INSERT INTO control_dw.ctrl_pipeline_log
(run_id, layer_name, object_name, status, ended_at, row_count, message)
VALUES
(@run_id, 'SILVER', 'silver_dw.erp_px_cat_g1v2', 'FINISHED', NOW(6), @rows, 'upsert completed');

SELECT 'SILVER LOAD FINISHED' AS status, @run_id AS run_id, @run_ts AS run_ts;