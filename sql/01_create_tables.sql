CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.stores (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    region VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.sales_batch (
    sale_id VARCHAR(100) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    sale_timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.sales_clean (
    sale_id VARCHAR(100) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    store_id VARCHAR(50) NOT NULL,
    store_name VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(100),
    customer_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    sale_timestamp TIMESTAMP NOT NULL,
    sale_date DATE NOT NULL,
    sale_hour INTEGER NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.sales_stream (
    sale_id VARCHAR(100) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    sale_timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.daily_sales_summary (
    sale_date DATE,
    store_id VARCHAR(50),
    store_name VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(100),
    total_revenue NUMERIC(12, 2),
    total_orders INTEGER,
    total_items_sold INTEGER,
    average_basket NUMERIC(10, 2),
    PRIMARY KEY (sale_date, store_id)
);

CREATE TABLE IF NOT EXISTS analytics.product_performance (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    total_revenue NUMERIC(12, 2),
    total_quantity_sold INTEGER,
    number_of_orders INTEGER,
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS analytics.realtime_sales_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_revenue NUMERIC(12, 2),
    total_orders INTEGER,
    average_basket NUMERIC(10, 2)
);