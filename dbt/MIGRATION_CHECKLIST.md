# ✅ Data Warehouse Migration Checklist

## 📋 Pre-Migration Setup

### Environment Setup
- [ ] Python 3.8+ installed
- [ ] Virtual environment created (`python3 -m venv venv`)
- [ ] Virtual environment activated (`source venv/bin/activate`)
- [ ] dbt-mysql installed (`pip install dbt-mysql`)
- [ ] dbt version verified (`dbt --version`)

### Database Access
- [ ] MySQL database accessible
- [ ] Database user has SELECT on `bronze_dw.*`
- [ ] Database user has ALL on `silver_dw.*`
- [ ] Database user has ALL on `gold_dw.*`
- [ ] Test connection works (`mysql -u user -p`)

### Configuration
- [ ] `profiles.yml` created in `~/.dbt/` or project root
- [ ] Database credentials updated in `profiles.yml`
- [ ] Database name updated in `models/sources/_sources.yml`
- [ ] `profiles.yml` added to `.gitignore`

---

## 🔧 Initial Setup

### dbt Installation
- [ ] Navigate to `dw_dbt` directory
- [ ] Run `dbt deps` successfully
- [ ] Run `dbt debug` - all checks pass
- [ ] Connection test shows "OK"

### Source Validation
- [ ] All bronze_dw tables exist in MySQL:
  - [ ] `crm_cust_info`
  - [ ] `crm_prd_info`
  - [ ] `crm_sales_details`
  - [ ] `erp_cust_az12`
  - [ ] `erp_loc_a101`
  - [ ] `erp_px_cat_g1v2`
- [ ] Column names in sources match actual MySQL tables
- [ ] Source freshness checks configured (optional)

---

## 🚀 First Run (Full Refresh)

### Staging Layer
- [ ] Run: `dbt run --select staging --full-refresh`
- [ ] All 6 staging models complete successfully:
  - [ ] `stg_crm_cust_info`
  - [ ] `stg_crm_prd_info`
  - [ ] `stg_crm_sales_details`
  - [ ] `stg_erp_cust_az12`
  - [ ] `stg_erp_loc_a101`
  - [ ] `stg_erp_px_cat_g1v2`
- [ ] Check row counts match expectations
- [ ] Verify data in `silver_dw` schema

### Intermediate Layer
- [ ] Run: `dbt run --select intermediate --full-refresh`
- [ ] Both intermediate models complete:
  - [ ] `int_customers_enriched`
  - [ ] `int_products_enriched`
- [ ] Verify joins worked correctly
- [ ] Check for NULL values in join keys

### Gold Layer - Dimensions
- [ ] Run: `dbt run --select dim_customers --full-refresh`
- [ ] Run: `dbt run --select dim_products --full-refresh`
- [ ] Dimensions created in `gold_dw` schema
- [ ] All records have `is_current = true` (initial load)
- [ ] `valid_from` populated correctly
- [ ] `valid_to = 9999-12-31` for all records
- [ ] Surrogate keys generated uniquely

### Gold Layer - Facts
- [ ] Run: `dbt run --select fact_sales --full-refresh`
- [ ] Fact table created in `gold_dw` schema
- [ ] Row count matches source sales data
- [ ] All `customer_key` values exist in `dim_customers`
- [ ] All `product_key` values exist in `dim_products`

---

## 🧪 Testing & Validation

### Built-in Tests
- [ ] Run: `dbt test`
- [ ] All uniqueness tests pass
- [ ] All not_null tests pass
- [ ] All relationship tests pass (FK → PK)

### Custom Tests
- [ ] `assert_no_orphaned_customer_keys` passes
- [ ] `assert_no_orphaned_product_keys` passes
- [ ] `assert_single_current_customer` passes
- [ ] `assert_single_current_product` passes
- [ ] `assert_no_overlapping_customer_dates` passes

### Data Quality Checks (Manual)
```sql
-- 1. Check dimension row counts
SELECT 'dim_customers' as table_name, COUNT(*) as row_count 
FROM gold_dw.dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) 
FROM gold_dw.dim_products
UNION ALL
SELECT 'fact_sales', COUNT(*) 
FROM gold_dw.fact_sales;

-- 2. Verify all facts have valid dimensions
SELECT 
    COUNT(*) as total_sales,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT f.product_key) as unique_products
FROM gold_dw.fact_sales f;

-- 3. Check for orphaned records
SELECT 'Orphaned Customers' as issue, COUNT(*) as count
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL
UNION ALL
SELECT 'Orphaned Products', COUNT(*)
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- 4. Verify SCD2 - all current flags
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current_records,
    SUM(CASE WHEN is_current = false THEN 1 ELSE 0 END) as historical_records
FROM gold_dw.dim_customers;
```

**Expected results:**
- [ ] All dimension/fact tables have rows
- [ ] No orphaned records (count = 0)
- [ ] All customers have `is_current = true` after initial load
- [ ] All products have `is_current = true` after initial load

---

## 🔄 Incremental Load Testing

### Simulate Data Change
```sql
-- Update a customer in bronze layer
UPDATE bronze_dw.crm_cust_info 
SET cst_firstname = 'UPDATED' 
WHERE cst_id = (SELECT MIN(cst_id) FROM bronze_dw.crm_cust_info);
```

### Run Incremental
- [ ] Run: `dbt run`
- [ ] Staging layer updates
- [ ] Intermediate layer updates
- [ ] `dim_customers` creates new version:
  - [ ] Old version has `is_current = false`
  - [ ] Old version has `valid_to = current_timestamp()`
  - [ ] New version has `is_current = true`
  - [ ] New version has `valid_to = 9999-12-31`
- [ ] Customer now has 2 versions in dimension

### Verify SCD2 Logic
```sql
-- Check customer versions
SELECT 
    customer_id,
    customer_key,
    first_name,
    valid_from,
    valid_to,
    is_current
FROM gold_dw.dim_customers
WHERE customer_id = [test_customer_id]
ORDER BY valid_from;
```

**Expected:**
- [ ] 2 rows for test customer
- [ ] 1 row with `is_current = false` (old version)
- [ ] 1 row with `is_current = true` (new version)
- [ ] `valid_to` of old = `valid_from` of new

---

## 📊 Analytics Validation

### Run Sample Queries
- [ ] Top 10 customers by revenue works
- [ ] Monthly sales trend works
- [ ] Sales by country works
- [ ] Product performance works
- [ ] Customer segment analysis works

### Point-in-Time Join Validation
```sql
-- Verify fact joins correct dimension version
SELECT 
    f.order_number,
    f.order_date,
    c.customer_key,
    c.first_name,
    c.valid_from,
    c.valid_to,
    c.is_current
FROM gold_dw.fact_sales f
JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE f.order_number = '[test_order]';
```

**Expected:**
- [ ] Order date falls between `valid_from` and `valid_to`
- [ ] Correct customer version joined

---

## 📚 Documentation

- [ ] Run: `dbt docs generate`
- [ ] Run: `dbt docs serve`
- [ ] Visit: `http://localhost:8080`
- [ ] Lineage graph shows all models
- [ ] Model descriptions visible
- [ ] Column descriptions visible
- [ ] Test results visible
- [ ] Source definitions visible

---

## 🛠 Performance & Optimization

### Query Performance
```sql
-- Check execution times
EXPLAIN SELECT 
    c.customer_number,
    SUM(f.total_amount) as revenue
FROM gold_dw.fact_sales f
JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.customer_number;
```

### Add Indexes (if needed)
```sql
-- Fact table indexes
CREATE INDEX idx_fact_sales_customer_key ON gold_dw.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product_key ON gold_dw.fact_sales(product_key);
CREATE INDEX idx_fact_sales_order_date ON gold_dw.fact_sales(order_date);

-- Dimension indexes
CREATE INDEX idx_dim_customers_is_current ON gold_dw.dim_customers(is_current);
CREATE INDEX idx_dim_customers_customer_id ON gold_dw.dim_customers(customer_id);
CREATE INDEX idx_dim_products_is_current ON gold_dw.dim_products(is_current);
CREATE INDEX idx_dim_products_product_id ON gold_dw.dim_products(product_id);
```

- [ ] Indexes added to fact table
- [ ] Indexes added to dimensions
- [ ] Query performance acceptable

---

## 🔐 Security & Access Control

### User Permissions
```sql
-- Create read-only user for BI tools
CREATE USER 'bi_reader'@'%' IDENTIFIED BY 'secure_password';
GRANT SELECT ON gold_dw.* TO 'bi_reader'@'%';

-- Create dbt user with write access
CREATE USER 'dbt_user'@'%' IDENTIFIED BY 'secure_password';
GRANT SELECT ON bronze_dw.* TO 'dbt_user'@'%';
GRANT ALL ON silver_dw.* TO 'dbt_user'@'%';
GRANT ALL ON gold_dw.* TO 'dbt_user'@'%';
```

- [ ] Read-only user created for BI tools
- [ ] dbt user has appropriate permissions
- [ ] Production credentials secured (not in repo)

---

## 🚀 Production Deployment

### Orchestration Setup
- [ ] Airflow/Prefect DAG created
- [ ] Schedule configured (daily/hourly)
- [ ] Error notifications configured
- [ ] Monitoring dashboard setup

### Environment Variables
```bash
export DBT_MYSQL_HOST=prod-mysql-host
export DBT_MYSQL_PORT=3306
export DBT_MYSQL_USER=dbt_user
export DBT_MYSQL_PASSWORD=secure_password
export DBT_MYSQL_DATABASE=prod_database
```

- [ ] Environment variables set
- [ ] Production profile configured
- [ ] CI/CD pipeline setup (optional)

### Monitoring
- [ ] dbt Cloud monitoring (optional)
- [ ] Elementary data observability (optional)
- [ ] Custom alerting configured
- [ ] Log aggregation setup

---

## 📈 Post-Migration

### Data Validation (7 days)
- [ ] Day 1: Verify initial load
- [ ] Day 2-7: Monitor incremental loads
- [ ] Compare row counts: old vs new warehouse
- [ ] Compare query results: old vs new warehouse
- [ ] Validate SCD2 history is accurate

### Stakeholder Sign-off
- [ ] Data team validated
- [ ] Analytics team validated
- [ ] Business stakeholders validated
- [ ] Query performance acceptable
- [ ] Documentation complete

### Knowledge Transfer
- [ ] Team trained on dbt
- [ ] Runbooks created
- [ ] Troubleshooting guide created
- [ ] On-call procedures defined

---

## 🎯 Success Criteria

✅ **Technical Success:**
- [ ] All models run successfully
- [ ] All tests pass
- [ ] SCD2 logic works correctly
- [ ] Incremental loads work
- [ ] Performance acceptable
- [ ] Documentation complete

✅ **Business Success:**
- [ ] Reports match old warehouse
- [ ] Queries run faster or same speed
- [ ] Historical data preserved
- [ ] Analytics team can use new warehouse
- [ ] Stakeholders satisfied

---

## 🚨 Rollback Plan

If migration fails:

1. **Identify Issue:**
   - [ ] Check dbt logs: `logs/dbt.log`
   - [ ] Check MySQL error logs
   - [ ] Review test failures

2. **Rollback Steps:**
   - [ ] Keep old warehouse running in parallel
   - [ ] Point BI tools back to old warehouse
   - [ ] Fix issues in dbt project
   - [ ] Re-run migration

3. **Common Issues & Fixes:**
   - Connection errors → Check `profiles.yml`
   - Source not found → Check `_sources.yml`
   - SCD2 not working → Run `--full-refresh`
   - Tests failing → Check data quality in source

---

## 📞 Support Contacts

- **dbt Issues:** https://community.getdbt.com
- **MySQL Issues:** DBA team
- **Project Lead:** [Your name]
- **On-call:** [On-call rotation]

---

**Migration Status:** [  ] Not Started  [  ] In Progress  [  ] Complete

**Migrated by:** ________________

**Date:** ________________

**Sign-off:** ________________

---

*Good luck with your migration! 🚀*
