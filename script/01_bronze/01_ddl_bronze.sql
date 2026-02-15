-- ============================================================================
-- bronze_dw.01_ddl_bronze.sql
-- Purpose:
--   Bronze layer stores raw source data in a consistent schema.
--   In this project, Bronze is FULL LOAD (truncate + reload).
--
-- Design choices:
--   - Each table includes ingestion_ts (DATETIME(6)) populated at load time.
--   - Date fields are nullable to avoid '0000-00-00' issues.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS bronze_dw;

-- ----------------------------------------------------------------------------
-- CRM - Customer information
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze_dw.crm_cust_info;
CREATE TABLE bronze_dw.crm_cust_info (
  cst_id            INT           NOT NULL,
  cst_key           VARCHAR(50)   NULL,
  cst_firstname     VARCHAR(100)  NULL,
  cst_lastname      VARCHAR(100)  NULL,
  cst_marital_status VARCHAR(10)  NULL,
  cst_gndr          VARCHAR(10)   NULL,
  cst_create_date   DATE          NULL,
  ingestion_ts      DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (cst_id)
);

-- ----------------------------------------------------------------------------
-- CRM - Product information
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze_dw.crm_prd_info;
CREATE TABLE bronze_dw.crm_prd_info (
  prd_id        INT          NOT NULL,
  cat_id        VARCHAR(50)  NULL,
  prd_key       VARCHAR(50)  NULL,
  prd_nm        VARCHAR(255) NULL,
  prd_cost      INT          NULL,
  prd_line      VARCHAR(50)  NULL,
  prd_start_dt  DATE         NULL,
  prd_end_dt    DATE         NULL,
  ingestion_ts  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (prd_id)
);

-- ----------------------------------------------------------------------------
-- CRM - Sales details
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze_dw.crm_sales_details;
CREATE TABLE bronze_dw.crm_sales_details (
  sls_ord_num   VARCHAR(50)  NOT NULL,
  sls_prd_key   VARCHAR(50)  NOT NULL,
  sls_cust_id   INT          NULL,
  sls_order_dt  DATE         NULL,
  sls_ship_dt   DATE         NULL,
  sls_due_dt    DATE         NULL,
  sls_sales     DECIMAL(18,2) NULL,
  sls_quantity  INT          NULL,
  sls_price     DECIMAL(18,2) NULL,
  ingestion_ts  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (sls_ord_num, sls_prd_key)
);

-- ----------------------------------------------------------------------------
-- ERP - Customer attributes (AZ12)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze_dw.erp_cust_az12;
CREATE TABLE bronze_dw.erp_cust_az12 (
  cid          VARCHAR(50)  NOT NULL,
  bdate        DATE         NULL,
  gen          VARCHAR(10)  NULL,
  ingestion_ts DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (cid)
);

-- ----------------------------------------------------------------------------
-- ERP - Customer location (A101)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze_dw.erp_loc_a101;
CREATE TABLE bronze_dw.erp_loc_a101 (
  cid          VARCHAR(50)  NOT NULL,
  cntry        VARCHAR(100) NULL,
  ingestion_ts DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (cid)
);

-- ----------------------------------------------------------------------------
-- ERP - Product category mapping (G1V2)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze_dw.erp_px_cat_g1v2;
CREATE TABLE bronze_dw.erp_px_cat_g1v2 (
  id           VARCHAR(50)  NOT NULL,
  cat          VARCHAR(255) NULL,
  subcat       VARCHAR(255) NULL,
  maintenance  VARCHAR(255) NULL,
  ingestion_ts DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id)
);
