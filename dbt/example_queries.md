# Example SQL queries to test your data warehouse

## 1. View current customers (SCD2)
```sql
SELECT 
    customer_key,
    customer_id,
    customer_number,
    first_name,
    last_name,
    country_name,
    segment
FROM gold_dw.dim_customers
WHERE is_current = true
ORDER BY customer_id;
```

## 2. View customer history (all versions)
```sql
SELECT 
    customer_key,
    customer_id,
    customer_number,
    first_name,
    last_name,
    country_name,
    valid_from,
    valid_to,
    is_current
FROM gold_dw.dim_customers
WHERE customer_id = 123  -- Replace with actual customer_id
ORDER BY valid_from DESC;
```

## 3. View current products
```sql
SELECT 
    product_key,
    product_id,
    product_number,
    product_name,
    category_name
FROM gold_dw.dim_products
WHERE is_current = true
ORDER BY product_id;
```

## 4. Sales analysis by customer
```sql
SELECT 
    c.customer_number,
    c.first_name,
    c.last_name,
    c.country_name,
    COUNT(DISTINCT f.order_number) as total_orders,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_revenue
FROM gold_dw.fact_sales f
JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
GROUP BY 
    c.customer_number,
    c.first_name,
    c.last_name,
    c.country_name
ORDER BY total_revenue DESC
LIMIT 10;
```

## 5. Sales analysis by product
```sql
SELECT 
    p.product_number,
    p.product_name,
    p.category_name,
    COUNT(DISTINCT f.order_number) as total_orders,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_revenue
FROM gold_dw.fact_sales f
JOIN gold_dw.dim_products p ON f.product_key = p.product_key
GROUP BY 
    p.product_number,
    p.product_name,
    p.category_name
ORDER BY total_revenue DESC
LIMIT 10;
```

## 6. Monthly sales trend
```sql
SELECT 
    DATE_FORMAT(order_date, '%Y-%m') as month,
    COUNT(DISTINCT order_number) as total_orders,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as total_revenue
FROM gold_dw.fact_sales
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY month DESC;
```

## 7. Sales by country
```sql
SELECT 
    c.country_name,
    c.region,
    COUNT(DISTINCT f.order_number) as total_orders,
    SUM(f.total_amount) as total_revenue
FROM gold_dw.fact_sales f
JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country_name, c.region
ORDER BY total_revenue DESC;
```

## 8. Verify SCD2 - Find customers with multiple versions
```sql
SELECT 
    customer_id,
    COUNT(*) as version_count,
    MIN(valid_from) as first_version,
    MAX(valid_from) as latest_version
FROM gold_dw.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC;
```

## 9. Check data quality - orphaned fact records
```sql
-- Facts without valid customer dimension
SELECT COUNT(*)
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Facts without valid product dimension  
SELECT COUNT(*)
FROM gold_dw.fact_sales f
LEFT JOIN gold_dw.dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;
```

## 10. Top customers by segment
```sql
SELECT 
    c.segment,
    c.customer_number,
    c.first_name,
    c.last_name,
    SUM(f.total_amount) as total_revenue
FROM gold_dw.fact_sales f
JOIN gold_dw.dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY 
    c.segment,
    c.customer_number,
    c.first_name,
    c.last_name
ORDER BY c.segment, total_revenue DESC;
```
