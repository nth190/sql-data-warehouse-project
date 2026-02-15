# Execution Guide (SQL Data Warehouse Project)

## Requirements
- MySQL 8.0+
- A SQL client (Navicat / MySQL Workbench / MySQL CLI)
- This repo cloned locally

## Project Structure (v2)
- `script_v2/00_init/ddl_control.sql`  
  Creates control schema and tables:
  - `control_dw.ctrl_watermark`
  - `control_dw.ctrl_pipeline_log`
  - `control_dw.ctrl_dq_log`

- `script_v2/01_bronze/*`  
  Bronze layer (full load from raw/source files)

- `script_v2/02_silver/*`  
  Silver layer (incremental upsert using watermark)

- `script_v2/03_gold/*`  
  Gold layer (incremental star schema + logging)

## How to Run
### Option 1 (Recommended): MySQL CLI
Run the pipeline runner:

```bash
mysql -u <user> -p < run_pipeline.sql