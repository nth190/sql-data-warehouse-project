# Data Warehouse Migration từ MySQL sang dbt - Step by Step Guide

## 📋 Prerequisites

1. **MySQL Database** với các schemas đã có:
   - `bronze_dw` (raw tables)
   - `silver_dw` (cleaned tables) 
   - `gold_dw` (dimensional model)
   - `control_dw` (control tables)

2. **Python 3.8+** installed
3. **MySQL connector** cho Python

## 🚀 Setup Instructions

### Bước 1: Install dbt-mysql

```bash
# Tạo virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux

# Install dbt-mysql
pip install dbt-mysql

# Verify installation
dbt --version
```

### Bước 2: Configure MySQL Connection

**Option A: Using profiles.yml (Local development)**

Tạo/edit file `~/.dbt/profiles.yml`:

```yaml
dw_mysql:
  target: dev
  outputs:
    dev:
      type: mysql
      host: localhost
      port: 3306
      user: your_mysql_user
      password: your_mysql_password
      database: your_database_name
      schema: silver_dw
      threads: 4
```

**Option B: Using environment variables (Production)**

```bash
export DBT_MYSQL_HOST=your_host
export DBT_MYSQL_PORT=3306
export DBT_MYSQL_USER=your_user
export DBT_MYSQL_PASSWORD=your_password
export DBT_MYSQL_DATABASE=your_database
```

### Bước 3: Update Source Configuration

Edit `models/sources/_sources.yml` và thay:
- `your_database_name` → tên database thực của bạn

### Bước 4: Install dbt Dependencies

```bash
cd dw_dbt
dbt deps
```

Lệnh này sẽ install:
- `dbt_utils` (for surrogate keys, generate_surrogate_key macro)
- `dbt_expectations` (for advanced data quality tests)

### Bước 5: Test Connection

```bash
dbt debug
```

Expected output:
```
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]

Required dependencies:
 - git [OK found]

Connection:
  host: localhost
  port: 3306
  database: your_database
  schema: silver_dw
  user: your_user
  Connection test: [OK connection ok]
```

### Bước 6: Run Initial Load (Full Refresh)

```bash
# Option 1: Run everything
dbt run --full-refresh

# Option 2: Run step by step
dbt run --select staging --full-refresh
dbt run --select intermediate --full-refresh  
dbt run --select gold --full-refresh
```

**Timeline estimate:**
- Staging: 2-5 phút (tùy data size)
- Intermediate: 1-2 phút
- Gold: 3-10 phút (SCD2 processing)

### Bước 7: Verify Results

```bash
# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

Visit http://localhost:8080 để xem lineage graph và documentation.

### Bước 8: Incremental Loads (Daily/Hourly)

```bash
# Run incremental updates (chỉ load new/changed data)
dbt run

# Or use the helper script
chmod +x run_dbt.sh
./run_dbt.sh incremental
```

## 📁 Project Structure

```
dw_dbt/
├── dbt_project.yml              # Project configuration
├── packages.yml                 # dbt packages (utils, expectations)
├── profiles.yml                 # Connection profile (local copy)
├── run_dbt.sh                   # Helper script
├── README.md                    # This file
├── SETUP_GUIDE.md              # Setup guide
├── example_queries.md          # Sample analytical queries
│
├── models/
│   ├── sources/
│   │   └── _sources.yml        # Source definitions (bronze_dw tables)
│   │
│   ├── staging/                # Bronze → Silver
│   │   ├── _staging.yml        # Model documentation & tests
│   │   ├── stg_crm_cust_info.sql
│   │   ├── stg_crm_prd_info.sql
│   │   ├── stg_crm_sales_details.sql
│   │   ├── stg_erp_cust_az12.sql
│   │   ├── stg_erp_loc_a101.sql
│   │   └── stg_erp_px_cat_g1v2.sql
│   │
│   ├── intermediate/           # Silver enrichment
│   │   ├── _intermediate.yml
│   │   ├── int_customers_enriched.sql
│   │   └── int_products_enriched.sql
│   │
│   └── gold/                   # Star Schema (Kimball)
│       ├── _gold.yml
│       ├── dim_customers.sql   # SCD Type 2
│       ├── dim_products.sql    # SCD Type 2
│       └── fact_sales.sql      # Fact table
│
└── macros/
    └── scd2_helpers.sql        # SCD2 utility macros
```

## 🔄 Data Flow

```
MySQL Bronze (Raw)
      ↓
   Staging (Clean + Validate)
      ↓
  Intermediate (Enrich + Join)
      ↓
   Gold Dimensions (SCD2)
      ↓
    Gold Facts (Star Schema)
```

## 🧪 Testing Strategy

### 1. Source Data Tests
```bash
# Test raw data quality
dbt test --select source:bronze_dw
```

### 2. Model Tests
```bash
# Test specific model
dbt test --select dim_customers

# Test all gold models
dbt test --select gold
```

### 3. Custom Tests
Add in `tests/` folder:
```sql
-- tests/assert_no_orphan_sales.sql
select *
from {{ ref('fact_sales') }} f
left join {{ ref('dim_customers') }} c on f.customer_key = c.customer_key
where c.customer_key is null
```

## 📊 Monitoring & Validation

### Check SCD2 Changes

```sql
-- View dimension changes
SELECT 
    customer_id,
    COUNT(*) as version_count,
    MIN(valid_from) as first_change,
    MAX(valid_from) as last_change
FROM gold_dw.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

### Data Quality Checks

```sql
-- Check fact-dim referential integrity
SELECT 
    'Customers' as dimension,
    COUNT(*) as orphan_count
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL

UNION ALL

SELECT 
    'Products' as dimension,
    COUNT(*) as orphan_count
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;
```

## 🛠 Troubleshooting

### Issue 1: dbt_utils not found
```bash
dbt deps
```

### Issue 2: MySQL connection failed
```bash
# Check MySQL is running
mysql -u your_user -p

# Verify host/port
telnet localhost 3306

# Check credentials in profiles.yml
dbt debug
```

### Issue 3: Schema not found
- Verify schema names trong MySQL:
  ```sql
  SHOW DATABASES;
  USE bronze_dw;
  SHOW TABLES;
  ```
- Update `_sources.yml` với correct schema names

### Issue 4: SCD2 not working properly
- Run full refresh first time:
  ```bash
  dbt run --select dim_customers --full-refresh
  ```
- Check columns in hash generation match actual columns
- Verify `valid_from`/`valid_to` datetime format

### Issue 5: Performance slow
- Add indexes to source tables:
  ```sql
  CREATE INDEX idx_customer_id ON bronze_dw.crm_cust_info(cst_id);
  CREATE INDEX idx_product_id ON bronze_dw.crm_prd_info(prd_id);
  CREATE INDEX idx_sales_date ON bronze_dw.crm_sales_details(sls_order_date);
  ```
- Increase threads in `profiles.yml`:
  ```yaml
  threads: 8  # Increase based on CPU cores
  ```

## 🔐 Security Best Practices

1. **Never commit credentials**
   - Add `profiles.yml` to `.gitignore`
   - Use environment variables for production

2. **Use read-only user for sources**
   ```sql
   CREATE USER 'dbt_reader'@'localhost' IDENTIFIED BY 'password';
   GRANT SELECT ON bronze_dw.* TO 'dbt_reader'@'localhost';
   ```

3. **Separate write user for target schemas**
   ```sql
   CREATE USER 'dbt_writer'@'localhost' IDENTIFIED BY 'password';
   GRANT ALL ON silver_dw.* TO 'dbt_writer'@'localhost';
   GRANT ALL ON gold_dw.* TO 'dbt_writer'@'localhost';
   ```

## 📈 Next Steps

### 1. Add Date Dimension
```sql
-- models/gold/dim_date.sql
{{ dbt_utils.date_spine(...) }}
```

### 2. Add More Tests
```yaml
# models/gold/_gold.yml
- name: fact_sales
  tests:
    - dbt_expectations.expect_table_row_count_to_be_between:
        min_value: 1000
```

### 3. Orchestration
- Setup Airflow DAG:
  ```python
  from airflow.operators.bash import BashOperator
  
  dbt_run = BashOperator(
      task_id='dbt_run',
      bash_command='cd /path/to/dw_dbt && dbt run'
  )
  ```

### 4. Add Snapshots
```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}
{{
    config(
      target_schema='snapshots',
      strategy='timestamp',
      unique_key='customer_id',
      updated_at='updated_at',
    )
}}
select * from {{ source('bronze_dw', 'crm_cust_info') }}
{% endsnapshot %}
```

## 📞 Support

- dbt Documentation: https://docs.getdbt.com
- dbt Slack Community: https://community.getdbt.com
- MySQL Connector: https://dev.mysql.com/doc/connector-python/en/

## 🎯 Success Criteria

✅ All source tables connected  
✅ Staging models clean data properly  
✅ Intermediate models enrich data  
✅ Dimensions implement SCD2 correctly  
✅ Fact table has referential integrity  
✅ All tests pass  
✅ Documentation generated  
✅ Incremental loads work  

Happy Data Engineering! 🚀
