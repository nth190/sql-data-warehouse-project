-- ============================================================================
-- Load Sample Data into Bronze Layer
-- ============================================================================
-- Purpose: Populate bronze_dw tables with realistic test data
-- Usage: mysql -u root -p bronze_dw < load_sample_data.sql
-- ============================================================================

-- Clear existing data (optional - comment out if you want to append)
-- TRUNCATE TABLE crm_cust_info;
-- TRUNCATE TABLE crm_prd_info;
-- TRUNCATE TABLE crm_sales_details;
-- TRUNCATE TABLE erp_cust_az12;
-- TRUNCATE TABLE erp_loc_a101;
-- TRUNCATE TABLE erp_px_cat_g1v2;

-- ============================================================================
-- 1. Load Customer Data (CRM)
-- ============================================================================
INSERT INTO crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) 
VALUES
(1, 'C001', 'John', 'Doe', 'M', 'M', '2024-01-01'),
(2, 'C002', 'Jane', 'Smith', 'S', 'F', '2024-01-05'),
(3, 'C003', 'Bob', 'Johnson', 'M', 'M', '2024-01-10'),
(4, 'C004', 'Alice', 'Brown', 'M', 'F', '2024-01-15'),
(5, 'C005', 'Charlie', 'Wilson', 'S', 'M', '2024-01-20'),
(6, 'C006', 'Diana', 'Moore', 'M', 'F', '2024-01-25'),
(7, 'C007', 'Edward', 'Taylor', 'M', 'M', '2024-02-01'),
(8, 'C008', 'Fiona', 'Anderson', 'S', 'F', '2024-02-05'),
(9, 'C009', 'George', 'Thomas', 'M', 'M', '2024-02-10'),
(10, 'C010', 'Hannah', 'Jackson', 'M', 'F', '2024-02-15');

-- ============================================================================
-- 2. Load Product Data (CRM)
-- ============================================================================
INSERT INTO crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, cat_id)
VALUES
(1, 'BK-R93R-62', 'Laptop Pro 15', 1200.00, 'Electronics', '2024-01-01', 1),
(2, 'BK-R93R-63', 'Wireless Mouse', 25.00, 'Accessories', '2024-01-05', 1),
(3, 'BK-R93R-64', 'USB-C Cable', 15.00, 'Accessories', '2024-01-10', 1),
(4, 'BK-R93R-65', 'Monitor 27inch', 350.00, 'Electronics', '2024-01-15', 1),
(5, 'BK-R93R-66', 'Keyboard Mechanical', 120.00, 'Accessories', '2024-01-20', 1),
(6, 'BK-R93R-67', 'Webcam HD', 80.00, 'Accessories', '2024-01-25', 1),
(7, 'BK-R93R-68', 'Desk Lamp LED', 45.00, 'Office', '2024-02-01', 2),
(8, 'BK-R93R-69', 'Office Chair', 250.00, 'Furniture', '2024-02-05', 2),
(9, 'BK-R93R-70', 'Desk Organizer', 30.00, 'Office', '2024-02-10', 2),
(10, 'BK-R93R-71', 'Cable Management', 20.00, 'Accessories', '2024-02-15', 1);

-- ============================================================================
-- 3. Load Sales Data (CRM)
-- ============================================================================
INSERT INTO crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
VALUES
(1001, 'BK-R93R-62', 1, '2024-02-01', '2024-02-02', '2024-02-10', 1200.00, 1, 1200.00),
(1002, 'BK-R93R-63', 2, '2024-02-05', '2024-02-06', '2024-02-15', 25.00, 1, 25.00),
(1002, 'BK-R93R-64', 2, '2024-02-05', '2024-02-06', '2024-02-15', 45.00, 3, 15.00),
(1003, 'BK-R93R-65', 3, '2024-02-10', '2024-02-11', '2024-02-20', 700.00, 2, 350.00),
(1004, 'BK-R93R-66', 4, '2024-02-12', '2024-02-13', '2024-02-22', 120.00, 1, 120.00),
(1005, 'BK-R93R-62', 5, '2024-02-15', '2024-02-16', '2024-02-25', 2400.00, 2, 1200.00),
(1006, 'BK-R93R-67', 6, '2024-02-18', '2024-02-19', '2024-02-28', 240.00, 3, 80.00),
(1007, 'BK-R93R-68', 7, '2024-02-20', '2024-02-21', '2024-03-02', 250.00, 1, 250.00),
(1008, 'BK-R93R-69', 8, '2024-02-22', '2024-02-23', '2024-03-04', 500.00, 2, 250.00),
(1009, 'BK-R93R-63', 9, '2024-02-25', '2024-02-26', '2024-03-07', 75.00, 3, 25.00),
(1010, 'BK-R93R-70', 10, '2024-02-28', '2024-03-01', '2024-03-10', 30.00, 1, 30.00),
(1011, 'BK-R93R-62', 1, '2024-03-01', '2024-03-02', '2024-03-12', 1200.00, 1, 1200.00),
(1012, 'BK-R93R-71', 2, '2024-03-03', '2024-03-04', '2024-03-14', 60.00, 3, 20.00);

-- ============================================================================
-- 4. Load ERP Customer Data
-- ============================================================================
INSERT INTO erp_cust_az12 (cid, gen, bdate, ingestion_ts)
VALUES
(1, 'M', '1990-01-15', NOW()),
(2, 'F', '1992-05-20', NOW()),
(3, 'M', '1988-12-10', NOW()),
(4, 'F', '1995-03-08', NOW()),
(5, 'M', '1991-07-22', NOW()),
(6, 'F', '1993-11-30', NOW()),
(7, 'M', '1989-06-14', NOW()),
(8, 'F', '1994-09-25', NOW()),
(9, 'M', '1987-02-18', NOW()),
(10, 'F', '1996-04-12', NOW());

-- ============================================================================
-- 5. Load Location Data
-- ============================================================================
INSERT INTO erp_loc_a101 (cid, cntry, ingestion_ts)
VALUES
(1, 'US', NOW()),
(2, 'UK', NOW()),
(3, 'CA', NOW()),
(4, 'DE', NOW()),
(5, 'FR', NOW()),
(6, 'AU', NOW()),
(7, 'JP', NOW()),
(8, 'SG', NOW()),
(9, 'IN', NOW()),
(10, 'BR', NOW());

-- ============================================================================
-- 6. Load Product Categories
-- ============================================================================
INSERT INTO erp_px_cat_g1v2 (id, cat, subcat, maintenance, ingestion_ts)
VALUES
(1, 'Electronics', 'Computers', 'Standard', NOW()),
(2, 'Electronics', 'Accessories', 'Basic', NOW()),
(3, 'Office', 'Furniture', 'Premium', NOW()),
(4, 'Office', 'Supplies', 'Standard', NOW());

-- ============================================================================
-- Verification Queries
-- ============================================================================
-- Uncomment these to verify data was loaded

-- SELECT 'crm_cust_info' as table_name, COUNT(*) as row_count FROM crm_cust_info
-- UNION ALL
-- SELECT 'crm_prd_info', COUNT(*) FROM crm_prd_info
-- UNION ALL
-- SELECT 'crm_sales_details', COUNT(*) FROM crm_sales_details
-- UNION ALL
-- SELECT 'erp_cust_az12', COUNT(*) FROM erp_cust_az12
-- UNION ALL
-- SELECT 'erp_loc_a101', COUNT(*) FROM erp_loc_a101
-- UNION ALL
-- SELECT 'erp_px_cat_g1v2', COUNT(*) FROM erp_px_cat_g1v2;

-- ============================================================================
-- Done!
-- ============================================================================
