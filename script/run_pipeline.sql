/* =============================================================================
   run_pipeline.sql
   Purpose:
     Run the full SQL Data Warehouse pipeline end-to-end (Control -> Bronze -> Silver -> Gold)

   How to use:
     - Open this file in your SQL client (Navicat / MySQL Workbench)
     - Execute the whole script in order

   Notes:
     - Control tables are created once in control_dw (watermark, pipeline log, dq log)
     - Bronze load is FULL load (as you decided)
     - Silver/Gold loads are INCREMENTAL using control_dw.ctrl_watermark
============================================================================= */

-- 0) CONTROL (watermark + logs)
SOURCE script_v2/00_init/ddl_control.sql;

-- 1) BRONZE (DDL + FULL load)
SOURCE script_v2/01_bronze/01_ddl_bronze.sql;
SOURCE script_v2/01_bronze/02_load_bronze.sql;

-- 2) SILVER (DDL + INCREMENTAL load)
SOURCE script_v2/02_silver/01_ddl_silver.sql;
SOURCE script_v2/02_silver/02_load_silver.sql;

-- 3) GOLD (DDL + INCREMENTAL load)
SOURCE script_v2/03_gold/01_ddl_gold.sql;
SOURCE script_v2/03_gold/02_load_gold.sql;

SELECT 'PIPELINE FINISHED' AS status, NOW(6) AS finished_at;