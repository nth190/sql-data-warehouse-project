-- ============================================================================
-- control_dw.ddl_control.sql
-- Purpose:
--   Central control tables used by ALL layers (Bronze/Silver/Gold):
--     1) ctrl_watermark     - incremental checkpoints per target table
--     2) ctrl_pipeline_log  - pipeline execution log (per run / per object)
--     3) ctrl_dq_log        - data quality results (per run / per rule)
--
-- Notes:
--   - Keep these tables in ONE schema to avoid duplication across layers.
--   - Timestamps use DATETIME(6) to preserve microseconds consistently.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS control_dw;
USE control_dw;

-- ----------------------------------------------------------------------------
-- 1) Watermark table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctrl_watermark (
  table_name   VARCHAR(200)  NOT NULL,           -- full name: e.g. 'silver_dw.crm_cust_info'
  last_ts      DATETIME(6)   NULL,               -- last processed business/technical timestamp
  updated_at   DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                     ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (table_name)
);

-- ----------------------------------------------------------------------------
-- 2) Pipeline execution log
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctrl_pipeline_log (
  id          BIGINT        NOT NULL AUTO_INCREMENT,
  run_id      VARCHAR(64)   NOT NULL,            -- same run_id reused across a pipeline run
  layer_name  VARCHAR(20)   NOT NULL,            -- BRONZE / SILVER / GOLD
  object_name VARCHAR(200)  NOT NULL,            -- e.g. 'silver_dw.crm_cust_info'
  status      VARCHAR(20)   NOT NULL,            -- STARTED / FINISHED / FAILED
  started_at  DATETIME(6)   NULL,
  ended_at    DATETIME(6)   NULL,
  row_count   INT           NULL,
  message     VARCHAR(500)  NULL,
  created_at  DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (id),
  INDEX idx_run (run_id),
  INDEX idx_layer_object (layer_name, object_name, started_at)
);

-- ----------------------------------------------------------------------------
-- 3) Data quality log
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctrl_dq_log (
  id                  BIGINT        NOT NULL AUTO_INCREMENT,
  run_id              VARCHAR(64)   NOT NULL,
  layer_name          VARCHAR(20)   NOT NULL,
  table_name          VARCHAR(200)  NOT NULL,    -- e.g. 'silver_dw.crm_cust_info'
  dq_rule_name        VARCHAR(200)  NOT NULL,    -- e.g. 'cst_id_not_null'
  dq_status           VARCHAR(20)   NOT NULL,    -- PASS / FAIL
  error_record_count  INT           NULL,
  total_row_count     INT           NULL,
  execution_ts        DATETIME(6)   NULL,
  created_at          DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (id),
  INDEX idx_dq_run   (run_id),
  INDEX idx_dq_table (layer_name, table_name, execution_ts)
);

-- ----------------------------------------------------------------------------
-- 4) Seed watermark values (safe to run multiple times)
--    These keys MUST match exactly what Silver/Gold scripts use.
-- ----------------------------------------------------------------------------
INSERT INTO ctrl_watermark (table_name, last_ts)
VALUES
  -- SILVER layer (CRM business dates)
  ('silver_dw.crm_cust_info',     '1900-01-01 00:00:00.000000'),
  ('silver_dw.crm_prd_info',      '1900-01-01 00:00:00.000000'),
  ('silver_dw.crm_sales_details', '1900-01-01 00:00:00.000000'),

  -- SILVER layer (ERP uses ingestion_ts)
  ('silver_dw.erp_cust_az12',     '1900-01-01 00:00:00.000000'),
  ('silver_dw.erp_loc_a101',      '1900-01-01 00:00:00.000000'),
  ('silver_dw.erp_px_cat_g1v2',   '1900-01-01 00:00:00.000000'),

  -- GOLD layer (dimensions & facts)
  ('gold_dw.dim_customers',       '1900-01-01 00:00:00.000000'),
  ('gold_dw.dim_products',        '1900-01-01 00:00:00.000000'),
  ('gold_dw.fact_sales',          '1900-01-01 00:00:00.000000')
ON DUPLICATE KEY UPDATE
  -- Keep existing watermark if already advanced by previous runs
  last_ts = last_ts;