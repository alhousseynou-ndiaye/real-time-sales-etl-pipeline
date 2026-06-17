from pathlib import Path
import os
import sys

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    count,
    sum as spark_sum,
    avg,
    round as spark_round,
)

# =========================================================
# ENV CONFIG
# =========================================================
load_dotenv()

# Important on Windows: force Spark to use the current venv Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "etl_sales_db")
DB_USER = os.getenv("POSTGRES_USER", "etl_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")


# =========================================================
# DATABASE HELPERS
# =========================================================
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def insert_dataframe(conn, table_name, columns, rows):
    if not rows:
        print(f"No rows to insert into {table_name}")
        return

    query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
    """

    values = [tuple(row.get(col_name) for col_name in columns) for row in rows]

    with conn.cursor() as cur:
        execute_values(cur, query, values)


# =========================================================
# MAIN SPARK JOB
# =========================================================
def main():
    print("Starting Spark transformations...")

    spark = (
        SparkSession.builder
        .appName("SalesSparkTransformations")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    products_path = str(RAW_DIR / "products.csv")
    stores_path = str(RAW_DIR / "stores.csv")
    sales_path = str(RAW_DIR / "sales_batch.csv")

    print(f"Reading products from: {products_path}")
    print(f"Reading stores from: {stores_path}")
    print(f"Reading sales from: {sales_path}")

    products = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(products_path)
        .withColumnRenamed("unit_price", "product_unit_price")
    )

    stores = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(stores_path)
    )

    sales = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(sales_path)
        .withColumn("sale_timestamp", to_timestamp(col("sale_timestamp")))
    )

    print("CSV files loaded successfully with Spark.")

    # =====================================================
    # 1. Clean and enrich sales data with Spark
    # =====================================================
    sales_clean = (
        sales
        .join(products, on="product_id", how="left")
        .join(stores, on="store_id", how="left")
        .withColumn("sale_date", to_date(col("sale_timestamp")))
        .withColumn("sale_hour", hour(col("sale_timestamp")))
        .select(
            col("sale_id"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("store_id"),
            col("store_name"),
            col("city"),
            col("region"),
            col("customer_id"),
            col("quantity"),
            col("unit_price"),
            col("total_amount"),
            col("payment_method"),
            col("sale_timestamp"),
            col("sale_date"),
            col("sale_hour"),
        )
    )

    # =====================================================
    # 2. Daily sales summary with Spark
    # PostgreSQL expects: total_items_sold, not total_quantity
    # =====================================================
    daily_sales_summary = (
        sales_clean
        .groupBy("sale_date", "store_id", "store_name", "city", "region")
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            count("sale_id").alias("total_orders"),
            spark_sum("quantity").alias("total_items_sold"),
            spark_round(avg("total_amount"), 2).alias("average_basket"),
        )
        .orderBy("sale_date", "store_id")
    )

    # =====================================================
    # 3. Product performance with Spark
    # PostgreSQL expects: total_quantity_sold and number_of_orders
    # =====================================================
    product_performance = (
        sales_clean
        .groupBy("product_id", "product_name", "category")
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity_sold"),
            count("sale_id").alias("number_of_orders"),
        )
        .orderBy(col("total_revenue").desc())
    )

    print("Spark transformations completed in memory.")

    sales_clean_rows = [row.asDict() for row in sales_clean.collect()]
    daily_rows = [row.asDict() for row in daily_sales_summary.collect()]
    product_rows = [row.asDict() for row in product_performance.collect()]

    print(f"Rows prepared for staging.sales_clean: {len(sales_clean_rows)}")
    print(f"Rows prepared for analytics.daily_sales_summary: {len(daily_rows)}")
    print(f"Rows prepared for analytics.product_performance: {len(product_rows)}")

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            print("Truncating target tables...")
            cur.execute("TRUNCATE TABLE staging.sales_clean;")
            cur.execute("TRUNCATE TABLE analytics.daily_sales_summary;")
            cur.execute("TRUNCATE TABLE analytics.product_performance;")

        print("Inserting staging.sales_clean...")
        insert_dataframe(
            conn,
            "staging.sales_clean",
            [
                "sale_id",
                "product_id",
                "product_name",
                "category",
                "store_id",
                "store_name",
                "city",
                "region",
                "customer_id",
                "quantity",
                "unit_price",
                "total_amount",
                "payment_method",
                "sale_timestamp",
                "sale_date",
                "sale_hour",
            ],
            sales_clean_rows,
        )

        print("Inserting analytics.daily_sales_summary...")
        insert_dataframe(
            conn,
            "analytics.daily_sales_summary",
            [
                "sale_date",
                "store_id",
                "store_name",
                "city",
                "region",
                "total_revenue",
                "total_orders",
                "total_items_sold",
                "average_basket",
            ],
            daily_rows,
        )

        print("Inserting analytics.product_performance...")
        insert_dataframe(
            conn,
            "analytics.product_performance",
            [
                "product_id",
                "product_name",
                "category",
                "total_revenue",
                "total_quantity_sold",
                "number_of_orders",
            ],
            product_rows,
        )

        conn.commit()

        print("Spark transformations loaded into PostgreSQL successfully.")
        print(f"staging.sales_clean rows: {len(sales_clean_rows)}")
        print(f"analytics.daily_sales_summary rows: {len(daily_rows)}")
        print(f"analytics.product_performance rows: {len(product_rows)}")

    except Exception as e:
        conn.rollback()
        print("Spark transformation failed.")
        raise e

    finally:
        conn.close()
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()