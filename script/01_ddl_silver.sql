/* ============================================================
   SILVER LAYER - TABLE DEFINITIONS (DDL) - MySQL
   Purpose:
     - Create curated SILVER tables used after cleansing and standardization
     - SILVER tables store business-friendly, quality-controlled data
     - Each table includes an audit column (dwh_create_date) to track load timestamp

   Notes:
     - DROP TABLE IF EXISTS ensures the script is re-runnable (idempotent)
     - dwh_create_date uses DATETIME(6) to capture microsecond precision
     - DEFAULT CURRENT_TIMESTAMP(6) automatically stamps insert time
   ============================================================ */


-- ============================================================
-- 1) silver_dw.crm_cust_info
-- Business meaning:
--   - Customer master data after cleansing (names, gender, marital status)
-- Technical notes:
--   - cst_id: customer identifier (int)
--   - cst_key: business/customer key from source system
--   - dwh_create_date: ETL audit timestamp for the row
-- ============================================================
DROP TABLE IF EXISTS silver_dw.crm_cust_info;

CREATE TABLE silver_dw.crm_cust_info (
  cst_id            INT,
  cst_key           VARCHAR(50),
  cst_firstname     VARCHAR(50),
  cst_lastname      VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr          VARCHAR(50),
  cst_create_date   DATE,

  -- NOTE: This column name appears to be product-related.
  -- If it is intentional, keep it; otherwise it may be a modeling typo.
  prd_end_dt        DATE,

  -- Audit column: timestamp when the row is created/loaded into the DWH
  dwh_create_date   DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


-- ============================================================
-- 2) silver_dw.crm_prd_info
-- Business meaning:
--   - Product master data (category, product line, effective dates)
-- Technical notes:
--   - cat_id is derived from product key (parsing/normalization in ETL)
--   - prd_start_dt/prd_end_dt support historization (validity period)
-- ============================================================
DROP TABLE IF EXISTS silver_dw.crm_prd_info;

CREATE TABLE silver_dw.crm_prd_info (
  prd_id           INT,
  cat_id           VARCHAR(50),
  prd_key          VARCHAR(50),
  prd_nm           VARCHAR(50),
  prd_cost         INT,
  prd_line         VARCHAR(50),
  prd_start_dt     DATE,
  prd_end_dt       DATE,

  -- Audit column: ETL load timestamp
  dwh_create_date  DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


-- ============================================================
-- 3) silver_dw.crm_sales_details
-- Business meaning:
--   - Sales transactional facts after validation and standardization
-- Technical notes:
--   - Dates are stored as DATE (already parsed in ETL)
--   - sls_sales, sls_quantity, sls_price are stored as INT in your model
--     (in real systems you might use DECIMAL for currency)
-- ============================================================
DROP TABLE IF EXISTS silver_dw.crm_sales_details;

CREATE TABLE silver_dw.crm_sales_details (
  sls_ord_num      VARCHAR(50),
  sls_prd_key      VARCHAR(50),
  sls_cust_id      INT,
  sls_order_dt     DATE,
  sls_ship_dt      DATE,
  sls_due_dt       DATE,
  sls_sales        INT,
  sls_quantity     INT,
  sls_price        INT,

  -- Audit column: ETL load timestamp
  dwh_create_date  DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


-- ============================================================
-- 4) silver_dw.erp_cust_az12
-- Business meaning:
--   - Customer attributes from ERP source (birth date, gender)
-- Technical notes:
--   - cid may contain prefixes (e.g., 'NAS...') that are cleaned in ETL
--   - Future birthdates are treated as invalid and set to NULL in ETL
-- ============================================================
DROP TABLE IF EXISTS silver_dw.erp_cust_az12;

CREATE TABLE silver_dw.erp_cust_az12 (
  cid             VARCHAR(50),
  bdate           DATE,
  gen             VARCHAR(50),

  -- Audit column: ETL load timestamp
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


-- ============================================================
-- 5) silver_dw.erp_loc_a101
-- Business meaning:
--   - Customer/location mapping from ERP source (country standardization)
-- Technical notes:
--   - cid is normalized (e.g., removing hyphens) in ETL
--   - cntry values standardized (DE -> Germany, US/USA -> United States, etc.)
-- ============================================================
DROP TABLE IF EXISTS silver_dw.erp_loc_a101;

CREATE TABLE silver_dw.erp_loc_a101 (
  cid             VARCHAR(50),
  cntry           VARCHAR(50),

  -- Audit column: ETL load timestamp
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


-- ============================================================
-- 6) silver_dw.erp_px_cat_g1v2
-- Business meaning:
--   - Product category taxonomy from ERP source
-- Technical notes:
--   - Typically used as a reference/lookup table (dimensions / attributes)
-- ============================================================
DROP TABLE IF EXISTS silver_dw.erp_px_cat_g1v2;

CREATE TABLE silver_dw.erp_px_cat_g1v2 (
  id              VARCHAR(50),
  cat             VARCHAR(50),
  subcat          VARCHAR(50),
  maintenance     VARCHAR(50),

  -- Audit column: ETL load timestamp
  dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);
