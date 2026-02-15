-- ============================================================================
-- control_dw.ddl_control.sql
-- Purpose:
--   Central control tables used by ALL layers (Bronze/Silver/Gold):
--     1) ctrl_watermark     - incremental checkpoints per target table
--     2) etl_pipeline_log   - pipeline execution log (per run / per step)
--     3) dq_quality_log     - data quality results (per run / per rule)
--
-- Notes:
--   - Keep these tables in ONE schema to avoid duplication across layers.
--   - Timestamps use DATETIME(6) to preserve microseconds consistently.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS control_dw;

-- ----------------------------------------------------------------------------
-- 1) Watermark table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control_dw.ctrl_watermark (
  table_name   VARCHAR(100)  NOT NULL,
  last_ts      DATETIME(6)   NOT NULL,
  updated_at   DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                              ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (table_name)
);

-- ----------------------------------------------------------------------------
-- 2) Pipeline execution log
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control_dw.etl_pipeline_log (
  log_id          BIGINT       NOT NULL AUTO_INCREMENT,
  run_id          BIGINT       NOT NULL,
  layer_name      VARCHAR(20)  NOT NULL,    -- BRONZE / SILVER / GOLD
  step_name       VARCHAR(100) NOT NULL,    -- e.g. load_bronze, load_silver_crm_cust_info
  status          VARCHAR(20)  NOT NULL,    -- STARTED / SUCCESS / FAILED
  start_ts        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  end_ts          DATETIME(6)  NULL,
  rows_affected   BIGINT       NULL,
  error_message   TEXT         NULL,
  PRIMARY KEY (log_id),
  INDEX idx_run (run_id),
  INDEX idx_layer_step (layer_name, step_name, start_ts)
);

-- ----------------------------------------------------------------------------
-- 3) Data quality log
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control_dw.dq_quality_log (
  dq_log_id          BIGINT       NOT NULL AUTO_INCREMENT,
  run_id             BIGINT       NOT NULL,
  layer_name         VARCHAR(20)  NOT NULL,
  table_name         VARCHAR(100) NOT NULL,
  dq_rule_name       VARCHAR(100) NOT NULL,
  dq_status          VARCHAR(10)  NOT NULL,    -- PASS / FAIL
  error_record_count BIGINT       NOT NULL DEFAULT 0,
  total_row_count    BIGINT       NOT NULL DEFAULT 0,
  execution_ts       DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (dq_log_id),
  INDEX idx_dq_run (run_id),
  INDEX idx_dq_table (layer_name, table_name, execution_ts)
);

-- ----------------------------------------------------------------------------
-- Seed watermark values (safe to run multiple times)
-- ----------------------------------------------------------------------------
INSERT INTO control_dw.ctrl_watermark (table_name, last_ts)
VALUES
  -- SILVER (CRM business dates)
  ('silver_crm_cust_info',   '1900-01-01 00:00:00.000000'),
  ('silver_crm_prd_info',    '1900-01-01 00:00:00.000000'),
  ('silver_crm_sales_details','1900-01-01 00:00:00.000000'),

  -- SILVER (ERP uses ingestion_ts)
  ('silver_erp_cust_az12',   '1900-01-01 00:00:00.000000'),
  ('silver_erp_loc_a101',    '1900-01-01 00:00:00.000000'),
  ('silver_erp_px_cat_g1v2', '1900-01-01 00:00:00.000000'),

  -- GOLD (dimensions/facts)
  ('gold_dim_customers',     '1900-01-01 00:00:00.000000'),
  ('gold_dim_products',      '1900-01-01 00:00:00.000000'),
  ('gold_fact_sales',        '1900-01-01 00:00:00.000000')
ON DUPLICATE KEY UPDATE
  last_ts = last_ts;  -- keep existing value
