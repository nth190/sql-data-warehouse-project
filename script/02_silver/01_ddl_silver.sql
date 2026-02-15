-- ============================================================================
-- bronze_dw.02_load_bronze.sql
-- Purpose:
--   Full load raw files into Bronze tables (truncate + reload).
--
-- How to use:
--   1) Update the file paths below to match your local environment.
--   2) Run this script in MySQL 8.0+ with LOCAL INFILE enabled.
--
-- Notes:
--   - ingestion_ts is set to the same @run_ts for all tables in the run.
--   - '0000-00-00' date strings are converted to NULL to avoid invalid dates.
-- ============================================================================

-- Generate a run identifier for logging (simple + MySQL-native)
SET @run_id := UUID_SHORT();
SET @run_ts := NOW(6);

-- Optional: record pipeline start (Bronze full load)
INSERT INTO control_dw.etl_pipeline_log (run_id, layer_name, step_name, status, start_ts)
VALUES (@run_id, 'BRONZE', 'load_bronze_full', 'STARTED', @run_ts);

-- ----------------------------------------------------------------------------
-- CRM_CUST_INFO
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze_dw.crm_cust_info;

LOAD DATA LOCAL INFILE '/ABSOLUTE/PATH/TO/datasets/source_crm/cust_info.csv'
INTO TABLE bronze_dw.crm_cust_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, @cst_create_date)
SET
  cst_create_date = NULLIF(@cst_create_date, '0000-00-00'),
  ingestion_ts    = @run_ts;

-- ----------------------------------------------------------------------------
-- CRM_PRD_INFO
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze_dw.crm_prd_info;

LOAD DATA LOCAL INFILE '/ABSOLUTE/PATH/TO/datasets/source_crm/prd_info.csv'
INTO TABLE bronze_dw.crm_prd_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
SET
  prd_start_dt = NULLIF(@prd_start_dt, '0000-00-00'),
  prd_end_dt   = NULLIF(@prd_end_dt, '0000-00-00'),
  ingestion_ts = @run_ts;

-- ----------------------------------------------------------------------------
-- CRM_SALES_DETAILS
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze_dw.crm_sales_details;

LOAD DATA LOCAL INFILE '/ABSOLUTE/PATH/TO/datasets/source_crm/sales_details.csv'
INTO TABLE bronze_dw.crm_sales_details
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(sls_ord_num, sls_prd_key, sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt,
 sls_sales, sls_quantity, sls_price)
SET
  sls_order_dt = NULLIF(@sls_order_dt, '0000-00-00'),
  sls_ship_dt  = NULLIF(@sls_ship_dt,  '0000-00-00'),
  sls_due_dt   = NULLIF(@sls_due_dt,   '0000-00-00'),
  ingestion_ts = @run_ts;

-- ----------------------------------------------------------------------------
-- ERP_CUST_AZ12
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze_dw.erp_cust_az12;

LOAD DATA LOCAL INFILE '/ABSOLUTE/PATH/TO/datasets/source_erp/cust_az12.csv'
INTO TABLE bronze_dw.erp_cust_az12
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cid, @bdate, gen)
SET
  bdate        = NULLIF(@bdate, '0000-00-00'),
  ingestion_ts = @run_ts;

-- ----------------------------------------------------------------------------
-- ERP_LOC_A101
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze_dw.erp_loc_a101;

LOAD DATA LOCAL INFILE '/ABSOLUTE/PATH/TO/datasets/source_erp/loc_a101.csv'
INTO TABLE bronze_dw.erp_loc_a101
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cid, cntry)
SET
  ingestion_ts = @run_ts;

-- ----------------------------------------------------------------------------
-- ERP_PX_CAT_G1V2
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze_dw.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/ABSOLUTE/PATH/TO/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze_dw.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, cat, subcat, maintenance)
SET
  ingestion_ts = @run_ts;

-- ----------------------------------------------------------------------------
-- Pipeline end (Bronze)
-- ----------------------------------------------------------------------------
UPDATE control_dw.etl_pipeline_log
SET status = 'SUCCESS',
    end_ts = NOW(6)
WHERE run_id = @run_id
  AND layer_name = 'BRONZE'
  AND step_name = 'load_bronze_full'
  AND status = 'STARTED';
