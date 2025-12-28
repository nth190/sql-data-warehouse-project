DROP PROCEDURE IF EXISTS bronze_dw.reset_bronze;
DELIMITER $$
CREATE PROCEDURE bronze_dw.reset_bronze()
BEGIN
  TRUNCATE TABLE bronze_dw.crm_cust_info;
  TRUNCATE TABLE bronze_dw.crm_prd_info;
  TRUNCATE TABLE bronze_dw.crm_sales_details;
  TRUNCATE TABLE bronze_dw.erp_loc_a101;
  TRUNCATE TABLE bronze_dw.erp_cust_az12;
  TRUNCATE TABLE bronze_dw.erp_px_cat_g1v2;
END$$
DELIMITER ;




DROP PROCEDURE IF EXISTS bronze_dw.check_bronze;

DELIMITER $$

CREATE PROCEDURE bronze_dw.check_bronze()
BEGIN
  SELECT 'crm_cust_info'      AS table_name, COUNT(*) AS row_count FROM bronze_dw.crm_cust_info
  UNION ALL
  SELECT 'crm_prd_info'       AS table_name, COUNT(*) AS row_count FROM bronze_dw.crm_prd_info
  UNION ALL
  SELECT 'crm_sales_details'  AS table_name, COUNT(*) AS row_count FROM bronze_dw.crm_sales_details
  UNION ALL
  SELECT 'erp_loc_a101'       AS table_name, COUNT(*) AS row_count FROM bronze_dw.erp_loc_a101
  UNION ALL
  SELECT 'erp_cust_az12'      AS table_name, COUNT(*) AS row_count FROM bronze_dw.erp_cust_az12
  UNION ALL
  SELECT 'erp_px_cat_g1v2'    AS table_name, COUNT(*) AS row_count FROM bronze_dw.erp_px_cat_g1v2;
END$$

DELIMITER ;
