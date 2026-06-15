import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"


def get_connection():
    load_dotenv()

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def load_products(connection):
    products_path = RAW_DATA_DIR / "products.csv"

    if not products_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {products_path}")

    df = pd.read_csv(products_path)

    records = df[
        [
            "product_id",
            "product_name",
            "category",
            "unit_price",
            "stock_quantity",
        ]
    ].values.tolist()

    query = """
        INSERT INTO raw.products (
            product_id,
            product_name,
            category,
            unit_price,
            stock_quantity
        )
        VALUES %s
        ON CONFLICT (product_id)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            unit_price = EXCLUDED.unit_price,
            stock_quantity = EXCLUDED.stock_quantity;
    """

    with connection.cursor() as cursor:
        execute_values(cursor, query, records)

    print(f"Produits chargés : {len(records)}")


def load_stores(connection):
    stores_path = RAW_DATA_DIR / "stores.csv"

    if not stores_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {stores_path}")

    df = pd.read_csv(stores_path)

    records = df[
        [
            "store_id",
            "store_name",
            "city",
            "region",
        ]
    ].values.tolist()

    query = """
        INSERT INTO raw.stores (
            store_id,
            store_name,
            city,
            region
        )
        VALUES %s
        ON CONFLICT (store_id)
        DO UPDATE SET
            store_name = EXCLUDED.store_name,
            city = EXCLUDED.city,
            region = EXCLUDED.region;
    """

    with connection.cursor() as cursor:
        execute_values(cursor, query, records)

    print(f"Magasins chargés : {len(records)}")


def load_sales_batch(connection):
    sales_path = RAW_DATA_DIR / "sales_batch.csv"

    if not sales_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {sales_path}")

    df = pd.read_csv(sales_path)

    records = df[
        [
            "sale_id",
            "product_id",
            "store_id",
            "customer_id",
            "quantity",
            "unit_price",
            "total_amount",
            "payment_method",
            "sale_timestamp",
        ]
    ].values.tolist()

    query = """
        INSERT INTO raw.sales_batch (
            sale_id,
            product_id,
            store_id,
            customer_id,
            quantity,
            unit_price,
            total_amount,
            payment_method,
            sale_timestamp
        )
        VALUES %s
        ON CONFLICT (sale_id)
        DO UPDATE SET
            product_id = EXCLUDED.product_id,
            store_id = EXCLUDED.store_id,
            customer_id = EXCLUDED.customer_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            total_amount = EXCLUDED.total_amount,
            payment_method = EXCLUDED.payment_method,
            sale_timestamp = EXCLUDED.sale_timestamp;
    """

    with connection.cursor() as cursor:
        execute_values(cursor, query, records)

    print(f"Ventes batch chargées : {len(records)}")


def main():
    print("Chargement des données batch vers PostgreSQL...")

    connection = get_connection()

    try:
        load_products(connection)
        load_stores(connection)
        load_sales_batch(connection)

        connection.commit()

        print("Chargement terminé avec succès ")

    except Exception as error:
        connection.rollback()
        print("Erreur pendant le chargement ")
        raise error

    finally:
        connection.close()


if __name__ == "__main__":
    main()