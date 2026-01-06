/*
===============================================================================
DDL: Create Gold Layer Views (Star Schema)
===============================================================================
Purpose
- Create Gold views for analytics and reporting.
- Gold is the final “business-ready” layer of the warehouse.
- The views below build a Star Schema:
  - Dimensions: customers, products
  - Fact table: sales

How it works
- Each view reads from the Silver layer.
- It cleans, standardizes, and enriches data using joins and simple rules.

Usage
- Query these views directly in BI tools (Power BI, Tableau) or SQL reports.
===============================================================================
*/

-- =============================================================================
-- Dimension View: gold_dw.dim_customers
-- =============================================================================
-- Goal
-- - One row per customer, enriched with country, gender, and birthdate.
-- - Create a surrogate key (customer_key) for the Star Schema.
-- Notes
-- - Start from CRM customer info (silver_dw.crm_cust_info).
-- - Join ERP tables to add missing attributes.
-- - Apply a simple rule to choose the best gender value.
DROP VIEW IF EXISTS gold_dw.dim_customers;

CREATE VIEW gold_dw.dim_customers AS
SELECT
  ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,     -- Surrogate key for analytics
  ci.cst_id          AS customer_id,                       -- Business/customer ID
  ci.cst_key         AS customer_number,                   -- Customer code/key used across systems
  ci.cst_firstname   AS first_name,
  ci.cst_lastname    AS last_name,
  la.cntry           AS country,
  ci.cst_marital_status AS marital_status,

  -- Gender selection rule (prefer the most reliable value)
  CASE
    WHEN ci.cst_gndr IS NULL THEN 'n/a'                    -- No gender in CRM
    WHEN ca.gen IS NULL OR ca.gen = 'n/a' THEN ci.cst_gndr -- ERP gender is missing → keep CRM
    WHEN ci.cst_gndr <> ca.gen THEN ca.gen                 -- Conflict → prefer ERP
    ELSE COALESCE(ca.gen, 'n/a')                           -- Default fallback
  END AS gender,

  ca.bdate           AS birthdate,
  ci.cst_create_date AS create_date

FROM silver_dw.crm_cust_info AS ci
LEFT JOIN silver_dw.erp_cust_az12 AS ca
  ON ci.cst_key = ca.cid                                   -- Add birthdate + ERP gender
LEFT JOIN silver_dw.erp_loc_a101 AS la
  ON ci.cst_key = la.cid;                                  -- Add country


-- =============================================================================
-- Dimension View: gold_dw.dim_products
-- =============================================================================
-- Goal
-- - One row per active product, enriched with category attributes.
-- - Create a surrogate key (product_key).
-- Notes
-- - Keep only active products (prd_end_dt IS NULL).
-- - Join category table to get category/subcategory/maintenance.
DROP VIEW IF EXISTS gold_dw.dim_products;

CREATE VIEW gold_dw.dim_products AS
SELECT
  ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
  pn.prd_id        AS product_id,
  pn.prd_key       AS product_number,
  pn.prd_nm        AS product_name,
  pn.cat_id        AS category_id,
  pc.cat           AS category,
  pc.subcat        AS subcategory,
  pc.maintenance   AS maintenance,
  pn.prd_cost      AS cost,
  pn.prd_line      AS product_line,
  pn.prd_start_dt  AS start_date
FROM silver_dw.crm_prd_info AS pn
LEFT JOIN silver_dw.erp_px_cat_g1v2 AS pc
  ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;                                -- Active products only


-- =============================================================================
-- Fact View: gold_dw.fact_sales
-- =============================================================================
-- Goal
-- - Sales transactions with links to customer and product dimensions.
-- Notes
-- - Join to dimensions to replace business IDs with surrogate keys.
-- - This is the main table for revenue, quantity, and price reporting.
DROP VIEW IF EXISTS gold_dw.fact_sales;

CREATE VIEW gold_dw.fact_sales AS
SELECT
  sd.sls_ord_num    AS order_number,
  pr.product_key    AS product_key,                         -- Link to dim_products
  cu.customer_key   AS customer_key,                        -- Link to dim_customers
  sd.sls_order_dt   AS order_date,
  sd.sls_ship_dt    AS shipping_date,
  sd.sls_due_dt     AS due_date,
  sd.sls_sales      AS sales_amount,
  sd.sls_quantity   AS quantity,
  sd.sls_price      AS price
FROM silver_dw.crm_sales_details AS sd
LEFT JOIN gold_dw.dim_products  AS pr
  ON sd.sls_prd_key  = pr.product_number                    -- Map product business key → surrogate key
LEFT JOIN gold_dw.dim_customers AS cu
  ON sd.sls_cust_id  = cu.customer_id;                      -- Map customer ID → surrogate key
