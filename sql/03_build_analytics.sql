TRUNCATE TABLE analytics.daily_sales_summary;
TRUNCATE TABLE analytics.product_performance;

INSERT INTO analytics.daily_sales_summary (
    sale_date,
    store_id,
    store_name,
    city,
    region,
    total_revenue,
    total_orders,
    total_items_sold,
    average_basket
)
SELECT
    sale_date,
    store_id,
    store_name,
    city,
    region,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(DISTINCT sale_id) AS total_orders,
    SUM(quantity) AS total_items_sold,
    ROUND(AVG(total_amount), 2) AS average_basket
FROM staging.sales_clean
GROUP BY
    sale_date,
    store_id,
    store_name,
    city,
    region;

INSERT INTO analytics.product_performance (
    product_id,
    product_name,
    category,
    total_revenue,
    total_quantity_sold,
    number_of_orders
)
SELECT
    product_id,
    product_name,
    category,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    SUM(quantity) AS total_quantity_sold,
    COUNT(DISTINCT sale_id) AS number_of_orders
FROM staging.sales_clean
GROUP BY
    product_id,
    product_name,
    category;