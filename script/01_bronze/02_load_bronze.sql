/* ============================================================
   Procedure: load_bronze (MySQL)
   Purpose:
     - Full refresh and ingest raw CSV data into BRONZE layer tables
     - For each table:
         1) TRUNCATE target
         2) LOAD DATA LOCAL INFILE from CSV
         3) Log execution metrics (start/end/duration/row_count)
     - Also logs total batch runtime
   Requirements:
     - local_infile must be enabled (client + server)
   ============================================================ */

DROP PROCEDURE IF EXISTS load_bronze;
DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN
  DECLARE batch_start DATETIME(6);
  DECLARE batch_end   DATETIME(6);
  DECLARE t0 DATETIME(6);
  DECLARE t1 DATETIME(6);

  -- Batch start time (overall ETL runtime tracking)
  SET batch_start = NOW(6);

  /* =========================
     1) bronze_dw.crm_cust_info
     ========================= */
  SET t0 = NOW(6);

  TRUNCATE TABLE bronze_dw.crm_cust_info;

  LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
  INTO TABLE bronze_dw.crm_cust_info
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  SET t1 = NOW(6);

  SELECT
    'crm_cust_info' AS table_name,
    t0 AS start_time,
    t1 AS end_time,
    TIMESTAMPDIFF(MICROSECOND, t0, t1)/1000 AS duration_ms,
    (SELECT COUNT(*) FROM bronze_dw.crm_cust_info) AS row_count;


  /* =========================
     2) bronze_dw.crm_prd_info
     ========================= */
  SET t0 = NOW(6);

  TRUNCATE TABLE bronze_dw.crm_prd_info;

  LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
  INTO TABLE bronze_dw.crm_prd_info
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  SET t1 = NOW(6);

  SELECT
    'crm_prd_info' AS table_name,
    t0 AS start_time,
    t1 AS end_time,
    TIMESTAMPDIFF(MICROSECOND, t0, t1)/1000 AS duration_ms,
    (SELECT COUNT(*) FROM bronze_dw.crm_prd_info) AS row_count;


  /* =============================
     3) bronze_dw.crm_sales_details
     ============================= */
  SET t0 = NOW(6);

  TRUNCATE TABLE bronze_dw.crm_sales_details;

  LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
  INTO TABLE bronze_dw.crm_sales_details
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  SET t1 = NOW(6);

  SELECT
    'crm_sales_details' AS table_name,
    t0 AS start_time,
    t1 AS end_time,
    TIMESTAMPDIFF(MICROSECOND, t0, t1)/1000 AS duration_ms,
    (SELECT COUNT(*) FROM bronze_dw.crm_sales_details) AS row_count;


  /* =========================
     4) bronze_dw.erp_cust_az12
     ========================= */
  SET t0 = NOW(6);

  TRUNCATE TABLE bronze_dw.erp_cust_az12;

  LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets//source_erp/cust_az12.csv'
  INTO TABLE bronze_dw.erp_cust_az12
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  SET t1 = NOW(6);

  SELECT
    'erp_cust_az12' AS table_name,
    t0 AS start_time,
    t1 AS end_time,
    TIMESTAMPDIFF(MICROSECOND, t0, t1)/1000 AS duration_ms,
    (SELECT COUNT(*) FROM bronze_dw.erp_cust_az12) AS row_count;


  /* =========================
     5) bronze_dw.erp_loc_a101
     ========================= */
  SET t0 = NOW(6);

  TRUNCATE TABLE bronze_dw.erp_loc_a101;

  LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
  INTO TABLE bronze_dw.erp_loc_a101
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  SET t1 = NOW(6);

  SELECT
    'erp_loc_a101' AS table_name,
    t0 AS start_time,
    t1 AS end_time,
    TIMESTAMPDIFF(MICROSECOND, t0, t1)/1000 AS duration_ms,
    (SELECT COUNT(*) FROM bronze_dw.erp_loc_a101) AS row_count;


  /* ============================
     6) bronze_dw.erp_px_cat_g1v2
     ============================ */
  SET t0 = NOW(6);

  TRUNCATE TABLE bronze_dw.erp_px_cat_g1v2;

  LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
  INTO TABLE bronze_dw.erp_px_cat_g1v2
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  SET t1 = NOW(6);

  SELECT
    'erp_px_cat_g1v2' AS table_name,
    t0 AS start_time,
    t1 AS end_time,
    TIMESTAMPDIFF(MICROSECOND, t0, t1)/1000 AS duration_ms,
    (SELECT COUNT(*) FROM bronze_dw.erp_px_cat_g1v2) AS row_count;


  -- Batch end log (total runtime across all steps)
  SET batch_end = NOW(6);

  SELECT
    'BATCH_TOTAL' AS table_name,
    batch_start AS start_time,
    batch_end AS end_time,
    TIMESTAMPDIFF(MICROSECOND, batch_start, batch_end)/1000 AS duration_ms,
    NULL AS row_count;

END$$

DELIMITER ;
