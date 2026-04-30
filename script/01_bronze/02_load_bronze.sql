/* ============================================================
   Script: 01_bronze/02_load_bronze.sql
   Purpose:
     - Full refresh BRONZE layer from local CSV files
     - Steps for each table:
         1) TRUNCATE target table
         2) LOAD DATA LOCAL INFILE from CSV
         3) Print simple runtime metrics (duration + row_count)
     - Also logs a high-level START / FINISHED entry into
       control_dw.ctrl_pipeline_log.

   Requirements:
     - MySQL 8.0+
     - LOCAL INFILE enabled on both client and server
   ============================================================ */

-- ------------------------------------------------------------
-- 0) Runtime & logging setup
-- ------------------------------------------------------------
USE bronze_dw;

SET @run_ts   := NOW(6);
SET @run_id   := DATE_FORMAT(@run_ts, '%Y%m%d%H%i%s%f');
SET @batch_start := NOW(6);

-- High-level pipeline START log
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
  'BRONZE',
  'bronze_dw.load_bronze_full',
  'STARTED',
  @run_ts,
  'Full reload from local CSV files'
);

-- =========================================================
-- 1) bronze_dw.crm_cust_info
-- =========================================================

SET @t0 := NOW(6);

TRUNCATE TABLE bronze_dw.crm_cust_info;

LOAD DATA LOCAL INFILE
  '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data_Engineering/Projects/Data_warehouse/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze_dw.crm_cust_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 LINES
(
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  @cst_create_date
)
SET
  -- Convert invalid date strings to NULL (defensive)
  cst_create_date = NULLIF(@cst_create_date, '0000-00-00');

SET @t1 := NOW(6);

SELECT
  'crm_cust_info' AS table_name,
  @t0             AS start_time,
  @t1             AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM bronze_dw.crm_cust_info) AS row_count;


-- =========================================================
-- 2) bronze_dw.crm_prd_info
-- =========================================================

SET @t0 := NOW(6);

TRUNCATE TABLE bronze_dw.crm_prd_info;

LOAD DATA LOCAL INFILE
  '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data_Engineering/Projects/Data_warehouse/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze_dw.crm_prd_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 LINES
(
  prd_id,
  cat_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  @prd_start_dt,
  @prd_end_dt
)
SET
  prd_start_dt = NULLIF(@prd_start_dt, '0000-00-00'),
  prd_end_dt   = NULLIF(@prd_end_dt,   '0000-00-00');

SET @t1 := NOW(6);

SELECT
  'crm_prd_info' AS table_name,
  @t0            AS start_time,
  @t1            AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM bronze_dw.crm_prd_info) AS row_count;


-- =========================================================
-- 3) bronze_dw.crm_sales_details
-- =========================================================

SET @t0 := NOW(6);

TRUNCATE TABLE bronze_dw.crm_sales_details;

LOAD DATA LOCAL INFILE
  '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data_Engineering/Projects/Data_warehouse/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze_dw.crm_sales_details
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 LINES
(
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  @sls_order_dt,
  @sls_ship_dt,
  @sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
)
SET
  sls_order_dt = NULLIF(@sls_order_dt, '0000-00-00'),
  sls_ship_dt  = NULLIF(@sls_ship_dt,  '0000-00-00'),
  sls_due_dt   = NULLIF(@sls_due_dt,   '0000-00-00');

SET @t1 := NOW(6);

SELECT
  'crm_sales_details' AS table_name,
  @t0                 AS start_time,
  @t1                 AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM bronze_dw.crm_sales_details) AS row_count;


-- =========================================================
-- 4) bronze_dw.erp_cust_az12
-- =========================================================

SET @t0 := NOW(6);

TRUNCATE TABLE bronze_dw.erp_cust_az12;

LOAD DATA LOCAL INFILE
  '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data_Engineering/Projects/Data_warehouse/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
INTO TABLE bronze_dw.erp_cust_az12
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 LINES
(
  cid,
  @bdate,
  gen
)
SET
  bdate = NULLIF(@bdate, '0000-00-00');

SET @t1 := NOW(6);

SELECT
  'erp_cust_az12' AS table_name,
  @t0             AS start_time,
  @t1             AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM bronze_dw.erp_cust_az12) AS row_count;


-- =========================================================
-- 5) bronze_dw.erp_loc_a101
-- =========================================================

SET @t0 := NOW(6);

TRUNCATE TABLE bronze_dw.erp_loc_a101;

LOAD DATA LOCAL INFILE
  '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data_Engineering/Projects/Data_warehouse/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
INTO TABLE bronze_dw.erp_loc_a101
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 LINES
(
  cid,
  cntry
);

SET @t1 := NOW(6);

SELECT
  'erp_loc_a101' AS table_name,
  @t0            AS start_time,
  @t1            AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM bronze_dw.erp_loc_a101) AS row_count;


-- =========================================================
-- 6) bronze_dw.erp_px_cat_g1v2
-- =========================================================

SET @t0 := NOW(6);

TRUNCATE TABLE bronze_dw.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE
  '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data_Engineering/Projects/Data_warehouse/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze_dw.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES  TERMINATED BY '\n'
IGNORE 1 LINES
(
  id,
  cat,
  subcat,
  maintenance
);

SET @t1 := NOW(6);

SELECT
  'erp_px_cat_g1v2' AS table_name,
  @t0               AS start_time,
  @t1               AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM bronze_dw.erp_px_cat_g1v2) AS row_count;


-- =========================================================
-- Batch total runtime + high-level FINISHED log
-- =========================================================

SET @batch_end := NOW(6);

-- Optional summary SELECT for quick check in the client
SELECT
  'BATCH_TOTAL' AS table_name,
  @batch_start  AS start_time,
  @batch_end    AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @batch_start, @batch_end)/1000 AS duration_ms,
  NULL          AS row_count;

-- Compute total rows across all Bronze tables (for logging)
SET @rows_total := (
    (SELECT COUNT(*) FROM bronze_dw.crm_cust_info)
  + (SELECT COUNT(*) FROM bronze_dw.crm_prd_info)
  + (SELECT COUNT(*) FROM bronze_dw.crm_sales_details)
  + (SELECT COUNT(*) FROM bronze_dw.erp_cust_az12)
  + (SELECT COUNT(*) FROM bronze_dw.erp_loc_a101)
  + (SELECT COUNT(*) FROM bronze_dw.erp_px_cat_g1v2)
);

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
  'BRONZE',
  'bronze_dw.load_bronze_full',
  'FINISHED',
  @batch_start,
  @batch_end,
  @rows_total,
  'Full reload completed'
);

SELECT COUNT(*) 
FROM bronze_dw.crm_cust_info;