# 🎯 Quick Reference - dbt Data Warehouse Project

## ⚡ Essential Commands

```bash
# Setup
dbt deps                     # Install packages
dbt debug                    # Test connection

# Run models
dbt run                      # Incremental run
dbt run --full-refresh       # Full refresh all
dbt run --select staging     # Run staging only
dbt run --select gold        # Run gold only

# Testing
dbt test                     # Run all tests
dbt test --select dim_customers

# Documentation
dbt docs generate            # Generate docs
dbt docs serve              # Serve docs (localhost:8080)

# Helper script
./run_dbt.sh all            # Complete pipeline
./run_dbt.sh staging        # Staging layer
./run_dbt.sh gold           # Gold layer
```

## 📊 Data Flow

```
Bronze (MySQL Raw)
    ↓
Silver (Staging + Intermediate)
    ↓
Gold (Star Schema: Dims + Facts)
```

## 🗂 Models

### Staging (6 models)
- `stg_crm_cust_info` - Customers from CRM
- `stg_crm_prd_info` - Products from CRM  
- `stg_crm_sales_details` - Sales transactions
- `stg_erp_cust_az12` - Customer attributes from ERP
- `stg_erp_loc_a101` - Country master data
- `stg_erp_px_cat_g1v2` - Product categories

### Intermediate (2 models)
- `int_customers_enriched` - Customers + ERP + Country
- `int_products_enriched` - Products + Categories

### Gold (3 models)
- `dim_customers` - Customer dimension (SCD2)
- `dim_products` - Product dimension (SCD2)
- `fact_sales` - Sales fact table

## 🔑 Key Features

✅ **SCD Type 2** - Track historical changes in dimensions  
✅ **Incremental Loading** - Only process new/changed data  
✅ **Data Quality Tests** - Automated validation  
✅ **Point-in-Time Joins** - Fact joins dims at order_date  
✅ **Full Documentation** - Auto-generated lineage  

## 📝 Configuration Files

- `dbt_project.yml` - Project settings
- `profiles.yml` - MySQL connection (LOCAL only!)
- `packages.yml` - dbt packages
- `models/sources/_sources.yml` - Source definitions
- `models/*/_*.yml` - Model docs & tests

## 🚨 Important Notes

1. **First Run**: Use `--full-refresh` for initial load
2. **Credentials**: Never commit `profiles.yml` to Git
3. **SCD2**: Dimensions track ALL changes with valid_from/valid_to
4. **Incremental**: Facts load only new orders after max(order_date)

## 📍 Files Location

```
dw_dbt/
├── models/
│   ├── sources/_sources.yml
│   ├── staging/stg_*.sql
│   ├── intermediate/int_*.sql
│   └── gold/{dim_*,fact_*}.sql
├── macros/scd2_helpers.sql
├── SETUP_GUIDE.md (detailed setup)
├── ARCHITECTURE.md (architecture overview)
└── example_queries.md (sample queries)
```

## 🔗 Helpful Links

- Setup Guide: `SETUP_GUIDE.md`
- Architecture: `ARCHITECTURE.md`  
- Example Queries: `example_queries.md`
- dbt Docs: https://docs.getdbt.com

---

**Need Help?** Check `SETUP_GUIDE.md` for troubleshooting! 🚀
