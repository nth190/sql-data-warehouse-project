/* ============================================================
   SILVER LAYER LOAD (ETL) - MySQL
   Purpose:
     - Full refresh the SILVER tables (TRUNCATE + INSERT)
     - Apply cleansing/standardization rules (TRIM, CASE mapping, null handling)
     - Keep only latest records for some entities (ROW_NUMBER / window function)
     - Compute validity ranges for product records (LEAD + DATE_SUB)
     - Record per-table execution audit: start/end time, duration, row_count
   Notes:
     - Using NOW(6) to capture microsecond precision timestamps
     - Logging is done via a SELECT statement after each load step
   ============================================================ */

-- Batch start time (overall ETL runtime tracking)
SET @batch_start := NOW(6);




/* ============================================================
   1) Load: silver_dw.crm_cust_info
   Strategy:
     - Full refresh target table
     - Standardize customer attributes (names, gender, marital status)
     - Keep only the most recent record per customer_id using ROW_NUMBER()
   ============================================================ */

SET @t0 := NOW(6);  -- Step start timestamp (microsecond precision)

TRUNCATE TABLE silver_dw.crm_cust_info;  -- Full refresh: remove all existing records

INSERT INTO silver_dw.crm_cust_info (
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date
)
SELECT
  cst_id,
  cst_key,

  -- Data cleansing: remove leading/trailing spaces in text fields
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname)  AS cst_lastname,

  -- Data standardization: map coded values to business-friendly labels
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

  cst_create_date
FROM
  (
    /* Dedup logic:
       - ROW_NUMBER ranks records per cst_id by create_date (latest first)
       - flag_last = 1 keeps the latest record for each cst_id
    */
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM silver_dw.crm_cust_info
  ) t
WHERE
  flag_last = 1
  AND cst_id != 0   -- Basic data quality filter: exclude invalid IDs
; -- IMPORTANT: end INSERT...SELECT statement before setting @t1

SET @t1 := NOW(6);  -- Step end timestamp

-- Per-table ETL audit log
SELECT
  'crm_cust_info' AS table_name,
  @t0 AS start_time,
  @t1 AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms, -- runtime in milliseconds
  (SELECT COUNT(*) FROM silver_dw.crm_cust_info) AS row_count;




/* ============================================================
   2) Load: silver_dw.crm_prd_info
   Strategy:
     - Full refresh target table
     - Parse product key into category id and cleaned product key
     - Standardize product line codes to descriptive labels
     - Create an end date by looking at the next start date (LEAD)
   ============================================================ */

SET @t0 := NOW(6);

TRUNCATE TABLE silver_dw.crm_prd_info;  -- FIXED: truncate correct target table

INSERT INTO silver_dw.crm_prd_info (
  prd_id,
  cat_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
)
SELECT
  prd_id,

  -- Extract category part from key and normalize separators
  REPLACE(SUBSTRING(PRD_KEY, 1, 5), '-', '_') AS cat_id,

  -- Remove prefix from product key; keep only the trailing identifier
  SUBSTRING(prd_key, 7, CHARACTER_LENGTH(prd_key)) AS prd_key,

  prd_nm,

  -- Null handling: default missing cost to 0
  IFNULL(prd_cost, 0) AS prd_cost,

  -- Standardize coded product line values
  CASE
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line,

  CAST(prd_start_dt AS DATE) AS prd_start_dt,

  /* Slowly changing product records:
     - LEAD(prd_start_dt) gives the next version's start date
     - Subtract 1 day to derive the current version's end date
  */
  DATE_SUB(
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
    INTERVAL 1 DAY
  ) AS prd_end_dt
FROM silver_dw.crm_prd_info
; -- IMPORTANT: end INSERT...SELECT before setting @t1

SET @t1 := NOW(6);

SELECT
  'crm_prd_info' AS table_name,
  @t0 AS start_time,
  @t1 AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM silver_dw.crm_prd_info) AS row_count;




/* ============================================================
   3) Load: silver_dw.crm_sales_details
   Strategy:
     - Full refresh target table
     - Convert integer-formatted dates (YYYYMMDD) into DATE using STR_TO_DATE
     - Validate/fix sales amount based on quantity * price
     - If price is missing/invalid, compute it from sales/quantity
   ============================================================ */

SET @t0 := NOW(6);

TRUNCATE TABLE silver_dw.crm_sales_details;

INSERT INTO silver_dw.crm_sales_details (
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
)
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,

  -- Date parsing with validation (must be 8 digits)
  CASE
    WHEN sls_order_dt = 0 OR CHAR_LENGTH(CAST(sls_order_dt AS CHAR)) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
  END AS sls_order_dt,

  CASE
    WHEN sls_ship_dt = 0 OR CHAR_LENGTH(CAST(sls_ship_dt AS CHAR)) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
  END AS sls_ship_dt,

  CASE
    WHEN sls_due_dt = 0 OR CHAR_LENGTH(CAST(sls_due_dt AS CHAR)) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
  END AS sls_due_dt,

  /* Sales validation:
     - If sales is null/<=0 or doesn't match qty * price, recompute it
     - Use ABS(price) to avoid negative values in source
  */
  CASE
    WHEN sls_sales IS NULL
      OR sls_sales <= 0
      OR sls_sales != COALESCE(sls_quantity,0) * ABS(COALESCE(sls_price,0))
    THEN COALESCE(sls_quantity,0) * ABS(COALESCE(sls_price,0))
    ELSE sls_sales
  END AS sls_sales,

  sls_quantity,

  -- Price validation: if missing/invalid, derive from sales divided by quantity
  CASE
    WHEN sls_price IS NULL OR sls_price <= 0
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
  END AS sls_price
FROM silver_dw.crm_sales_details
; -- IMPORTANT: end INSERT...SELECT before setting @t1

SET @t1 := NOW(6);

SELECT
  'crm_sales_details' AS table_name,
  @t0 AS start_time,
  @t1 AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM silver_dw.crm_sales_details) AS row_count;




/* ============================================================
   4) Load: silver_dw.erp_cust_az12
   Strategy:
     - Full refresh target table
     - Clean customer id format (remove NAS prefix if present)
     - Validate birthdate (future dates -> NULL)
     - Standardize gender values; remove carriage returns from source
   ============================================================ */

SET @t0 := NOW(6);

TRUNCATE TABLE silver_dw.erp_cust_az12;

INSERT INTO silver_dw.erp_cust_az12 (cid, bdate, gen)
SELECT
  -- Normalize CID: remove 'NAS' prefix when present
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, CHARACTER_LENGTH(cid))
    ELSE cid
  END AS cid,

  -- Birthdate data quality rule: future dates are invalid
  CASE
    WHEN bdate > CURDATE() THEN NULL
    ELSE bdate
  END AS bdate,

  -- Standardize gender values and remove embedded CR characters
  CASE
    WHEN UPPER(TRIM(REPLACE(gen, '\r', ''))) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(TRIM(REPLACE(gen, '\r', ''))) IN ('F', 'FEMALE') THEN 'Female'
    ELSE 'n/a'
  END AS gen
FROM silver_dw.erp_cust_az12
; -- IMPORTANT: end INSERT...SELECT before setting @t1

SET @t1 := NOW(6);

SELECT
  'erp_cust_az12' AS table_name,
  @t0 AS start_time,
  @t1 AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM silver_dw.erp_cust_az12) AS row_count;




/* ============================================================
   5) Load: silver_dw.erp_loc_a101
   Strategy:
     - Full refresh target table
     - Normalize customer id by removing hyphens
     - Standardize country codes to full country names
     - Handle blanks / nulls as 'n/a'
   ============================================================ */

SET @t0 := NOW(6);

TRUNCATE TABLE silver_dw.erp_loc_a101;

INSERT INTO silver_dw.erp_loc_a101 (cid, cntry)
SELECT
  REPLACE(cid, '-', '') AS cid,

  CASE
    WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, '\r',''), '\n',''))) = 'DE' THEN 'Germany'
    WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, '\r',''), '\n',''))) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(REPLACE(REPLACE(cntry, '\r',''), '\n','')) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(REPLACE(REPLACE(cntry, '\r',''), '\n',''))
  END AS cntry
FROM silver_dw.erp_loc_a101
; -- IMPORTANT: end INSERT...SELECT before setting @t1

SET @t1 := NOW(6);

SELECT
  'erp_loc_a101' AS table_name,
  @t0 AS start_time,
  @t1 AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM silver_dw.erp_loc_a101) AS row_count;




/* ============================================================
   6) Load: silver_dw.erp_px_cat_g1v2
   Strategy:
     - Full refresh target table
     - Direct load (no transformations in this step)
   ============================================================ */

SET @t0 := NOW(6);

TRUNCATE TABLE silver_dw.erp_px_cat_g1v2;

INSERT INTO silver_dw.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT
  id,
  cat,
  subcat,
  maintenance
FROM silver_dw.erp_px_cat_g1v2;

SET @t1 := NOW(6);

SELECT
  'erp_px_cat_g1v2' AS table_name,
  @t0 AS start_time,
  @t1 AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @t0, @t1)/1000 AS duration_ms,
  (SELECT COUNT(*) FROM silver_dw.erp_px_cat_g1v2) AS row_count;




/* ============================================================
   Batch end log (total runtime across all steps)
   ============================================================ */

SET @batch_end := NOW(6);

SELECT
  'BATCH_TOTAL' AS table_name,
  @batch_start AS start_time,
  @batch_end AS end_time,
  TIMESTAMPDIFF(MICROSECOND, @batch_start, @batch_end)/1000 AS duration_ms,
  NULL AS row_count;
