from pathlib import Path
import os
import subprocess
import sys

import psycopg2
from dotenv import load_dotenv

# =========================================================
# CONFIG
# =========================================================
load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "etl_sales_db")
DB_USER = os.getenv("POSTGRES_USER", "etl_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")


# =========================================================
# HELPERS
# =========================================================
def run_script(script_path: str) -> None:
    full_path = BASE_DIR / script_path

    print("=" * 70)
    print(f"Running: {script_path}")
    print("=" * 70)

    result = subprocess.run(
        [sys.executable, str(full_path)],
        cwd=BASE_DIR,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Script failed: {script_path}")

    print(f"Completed: {script_path}")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def print_table_counts() -> None:
    queries = {
        "raw.products": "SELECT COUNT(*) FROM raw.products;",
        "raw.stores": "SELECT COUNT(*) FROM raw.stores;",
        "raw.sales_batch": "SELECT COUNT(*) FROM raw.sales_batch;",
        "staging.sales_clean": "SELECT COUNT(*) FROM staging.sales_clean;",
        "analytics.daily_sales_summary": "SELECT COUNT(*) FROM analytics.daily_sales_summary;",
        "analytics.product_performance": "SELECT COUNT(*) FROM analytics.product_performance;",
    }

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            print("=" * 70)
            print("Final table counts")
            print("=" * 70)

            for table_name, query in queries.items():
                cur.execute(query)
                count = cur.fetchone()[0]
                print(f"{table_name}: {count}")

    finally:
        conn.close()


# =========================================================
# MAIN PIPELINE
# =========================================================
def main():
    print("Starting batch pipeline with PySpark transformations...")

    # 1. Generate synthetic CSV data
    run_script("scripts/generate_batch_data.py")

    # 2. Load raw CSV data into PostgreSQL
    run_script("scripts/load_batch_to_postgres.py")

    # 3. Transform data with PySpark and load staging + analytics tables
    run_script("scripts/spark_transformations.py")

    # 4. Validate final counts
    print_table_counts()

    print("=" * 70)
    print("Batch pipeline completed successfully with PySpark.")
    print("=" * 70)


if __name__ == "__main__":
    main()