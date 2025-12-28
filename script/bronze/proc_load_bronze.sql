TRUNCATE TABLE bronze_dw.crm_cust_info;
LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' 
INTO TABLE bronze_dw.crm_cust_info 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

TRUNCATE TABLE bronze_dw.crm_prd_info;
LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' 
INTO TABLE bronze_dw.crm_prd_info
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

TRUNCATE TABLE bronze_dw.crm_sales_details;
LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' 
INTO TABLE bronze_dw.crm_sales_details
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

TRUNCATE TABLE bronze_dw.erp_cust_az12;
LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets//source_erp/cust_az12.csv' 
INTO TABLE bronze_dw.erp_cust_az12
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

TRUNCATE TABLE bronze_dw.erp_loc_a101;
LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
INTO TABLE bronze_dw.erp_loc_a101
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

TRUNCATE TABLE bronze_dw.erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE '/Users/hieunguyen/Library/CloudStorage/OneDrive-Personal/Documents/Workspace/Data Engeneering/Projects/Data Warehouse project/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv' 
INTO TABLE bronze_dw.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;
