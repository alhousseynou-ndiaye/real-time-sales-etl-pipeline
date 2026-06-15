TRUNCATE TABLE staging.sales_clean;

INSERT INTO staging.sales_clean (
    sale_id,
    product_id,
    product_name,
    category,
    store_id,
    store_name,
    city,
    region,
    customer_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    sale_timestamp,
    sale_date,
    sale_hour
)
SELECT
    sb.sale_id,
    sb.product_id,
    p.product_name,
    p.category,
    sb.store_id,
    s.store_name,
    s.city,
    s.region,
    sb.customer_id,
    sb.quantity,
    sb.unit_price,
    sb.total_amount,
    sb.payment_method,
    sb.sale_timestamp,
    DATE(sb.sale_timestamp) AS sale_date,
    CAST(EXTRACT(HOUR FROM sb.sale_timestamp) AS INTEGER) AS sale_hour
FROM raw.sales_batch sb
LEFT JOIN raw.products p
    ON sb.product_id = p.product_id
LEFT JOIN raw.stores s
    ON sb.store_id = s.store_id
WHERE sb.sale_id IS NOT NULL
  AND sb.quantity > 0
  AND sb.total_amount >= 0;