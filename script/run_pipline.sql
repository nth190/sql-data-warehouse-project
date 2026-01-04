/* ============================================================
   Data Warehouse Pipeline Runner
   Purpose:
     - Execute the full Data Warehouse pipeline end-to-end
     - Run Bronze → Silver → Gold layers in the correct order

   Execution order:
     1) load_bronze()  → ingest raw data from source files
     2) load_silver()  → cleanse, standardize, and validate data
     3) load_gold()    → create business-ready tables / aggregates

   Notes:
     - Each procedure performs a full refresh (TRUNCATE + LOAD/INSERT)
     - Execution logs (start_time, end_time, duration, row_count)
       are returned by each procedure
   ============================================================ */

CALL load_bronze();
CALL load_silver();
CALL load_gold();

CALL run_all_layers();
