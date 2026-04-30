-- ============================================================================
-- silver_dw.01_ddl_silver.sql
-- Purpose:
--   Silver layer stores cleaned, conformed data for downstream Gold models.
--   - One schema: silver_dw
--   - One table per business entity, with clear primary/unique keys
--   - ingestion_ts used as batch timestamp for incremental logic
--
-- Design choices:
--   - dwh_create_ts: when the row was first created in Silver
--   - last_updated_ts: when the row was last updated in Silver
--   - ingestion_ts: batch timestamp coming from the load process
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver_dw;

-- ============================================================================
-- 1) SILVER - CRM_CUST_INFO
--    Source: bronze_dw.crm_cust_info
--    Grain: 1 row = 1 customer (cst_id)
-- ============================================================================

DROP TABLE IF EXISTS silver_dw.crm_cust_info;
CREATE TABLE silver_dw.crm_cust_info (
  -- Business key
  cst_id              INT          NOT NULL,           -- customer id from source CRM
  cst_key             VARCHAR(50)  NULL,              -- natural/business key if available

  -- Attributes
  cst_firstname       VARCHAR(100) NULL,
  cst_lastname        VARCHAR(100) NULL,
  cst_marital_status  VARCHAR(50)  NULL,              -- normalized to 'Single' / 'Married' / 'n/a'
  cst_gndr            VARCHAR(10)  NULL,              -- 'Male' / 'Female' / 'n/a'
  cst_create_date     DATE         NULL,              -- business create date from source

  -- Technical columns for the data warehouse
  dwh_create_ts       DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_updated_ts     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                            ON UPDATE CURRENT_TIMESTAMP(6),
  ingestion_ts        DATETIME(6)  NOT NULL,          -- batch timestamp from Silver load

  PRIMARY KEY (cst_id)
);


-- ============================================================================
-- 2) SILVER - CRM_PRD_INFO
--    Source: bronze_dw.crm_prd_info
--    Grain: 1 row = 1 product version (prd_id)
-- ============================================================================

DROP TABLE IF EXISTS silver_dw.crm_prd_info;
CREATE TABLE silver_dw.crm_prd_info (
  -- Business key
  prd_id         INT          NOT NULL,

  -- Attributes
  cat_id         VARCHAR(50)  NULL,
  prd_key        VARCHAR(50)  NULL,
  prd_nm         VARCHAR(255) NULL,
  prd_cost       INT          NULL,
  prd_line       VARCHAR(50)  NULL,
  prd_start_dt   DATE         NULL,                   -- business start date
  prd_end_dt     DATE         NULL,                   -- business end date (NULL = active)

  -- Technical columns
  dwh_create_ts  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_updated_ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                            ON UPDATE CURRENT_TIMESTAMP(6),
  ingestion_ts   DATETIME(6)  NOT NULL,              -- batch timestamp from Silver load

  PRIMARY KEY (prd_id)
);


-- ============================================================================
-- 3) SILVER - CRM_SALES_DETAILS
--    Source: bronze_dw.crm_sales_details
--    Grain: 1 row = 1 order line (order_number + product)
-- ============================================================================

DROP TABLE IF EXISTS silver_dw.crm_sales_details;
CREATE TABLE silver_dw.crm_sales_details (
  -- Business composite key
  sls_ord_num    VARCHAR(50)  NOT NULL,               -- order number
  sls_prd_key    VARCHAR(50)  NOT NULL,               -- product number from CRM
  sls_cust_id    INT          NULL,                   -- customer id

  -- Dates (cleaned: invalid '0000-00-00' handled at load time)
  sls_order_dt   DATE         NULL,
  sls_ship_dt    DATE         NULL,
  sls_due_dt     DATE         NULL,

  -- Measures
  sls_sales      DECIMAL(18,2) NULL,
  sls_quantity   INT           NULL,
  sls_price      DECIMAL(18,2) NULL,

  -- Technical columns
  dwh_create_ts  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_updated_ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                            ON UPDATE CURRENT_TIMESTAMP(6),
  ingestion_ts   DATETIME(6)  NOT NULL,              -- batch timestamp from Silver load

  PRIMARY KEY (sls_ord_num, sls_prd_key)
);


-- ============================================================================
-- 4) SILVER - ERP_CUST_AZ12
--    Source: bronze_dw.erp_cust_az12
--    Grain: 1 row = 1 customer (cid) with ERP attributes
--    Note:
--      - Business date (bdate) is NOT a create/update date,
--        so incremental logic uses ingestion_ts instead.
-- ============================================================================

DROP TABLE IF EXISTS silver_dw.erp_cust_az12;
CREATE TABLE silver_dw.erp_cust_az12 (
  -- Business key
  cid            VARCHAR(50)  NOT NULL,               -- customer id from ERP

  -- Attributes
  bdate          DATE         NULL,                   -- birthdate (can be NULL)
  gen            VARCHAR(10)  NULL,                   -- gender from ERP (may conflict with CRM)

  -- Technical columns
  dwh_create_ts  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_updated_ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                            ON UPDATE CURRENT_TIMESTAMP(6),
  ingestion_ts   DATETIME(6)  NULL,                   -- pass-through of Bronze ingestion_ts

  PRIMARY KEY (cid)
);


-- ============================================================================
-- 5) SILVER - ERP_LOC_A101
--    Source: bronze_dw.erp_loc_a101
--    Grain: 1 row = 1 customer location (cid)
--    Note:
--      - No business dates available, so incremental uses ingestion_ts.
-- ============================================================================

DROP TABLE IF EXISTS silver_dw.erp_loc_a101;
CREATE TABLE silver_dw.erp_loc_a101 (
  -- Business key
  cid            VARCHAR(50)  NOT NULL,

  -- Attributes
  cntry          VARCHAR(100) NULL,                   -- country code/text

  -- Technical columns
  dwh_create_ts  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_updated_ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                            ON UPDATE CURRENT_TIMESTAMP(6),
  ingestion_ts   DATETIME(6)  NULL,                   -- pass-through of Bronze ingestion_ts

  PRIMARY KEY (cid)
);


-- ============================================================================
-- 6) SILVER - ERP_PX_CAT_G1V2
--    Source: bronze_dw.erp_px_cat_g1v2
--    Grain: 1 row = 1 category mapping (id)
--    Note:
--      - No business dates available, so incremental uses ingestion_ts.
-- ============================================================================

DROP TABLE IF EXISTS silver_dw.erp_px_cat_g1v2;
CREATE TABLE silver_dw.erp_px_cat_g1v2 (
  -- Business key
  id             VARCHAR(50)  NOT NULL,

  -- Attributes
  cat            VARCHAR(255) NULL,
  subcat         VARCHAR(255) NULL,
  maintenance    VARCHAR(255) NULL,

  -- Technical columns
  dwh_create_ts  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_updated_ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                            ON UPDATE CURRENT_TIMESTAMP(6),
  ingestion_ts   DATETIME(6)  NULL,                   -- pass-through of Bronze ingestion_ts

  PRIMARY KEY (id)
);

-- ============================================================================
