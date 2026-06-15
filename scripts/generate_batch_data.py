import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker


fake = Faker("fr_FR")

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


PRODUCT_CATEGORIES = {
    "Electronics": [
        "Smartphone",
        "Laptop",
        "Tablet",
        "Wireless Headphones",
        "Smartwatch",
    ],
    "Home": [
        "Coffee Machine",
        "Vacuum Cleaner",
        "Desk Lamp",
        "Air Fryer",
        "Electric Kettle",
    ],
    "Fashion": [
        "Sneakers",
        "T-Shirt",
        "Jeans",
        "Jacket",
        "Backpack",
    ],
    "Beauty": [
        "Perfume",
        "Face Cream",
        "Shampoo",
        "Makeup Kit",
        "Body Lotion",
    ],
    "Food": [
        "Rice Bag",
        "Olive Oil",
        "Coffee Pack",
        "Chocolate Box",
        "Pasta Pack",
    ],
}


CITIES = [
    ("Paris", "Île-de-France"),
    ("Lyon", "Auvergne-Rhône-Alpes"),
    ("Marseille", "Provence-Alpes-Côte d'Azur"),
    ("Lille", "Hauts-de-France"),
    ("Bordeaux", "Nouvelle-Aquitaine"),
    ("Toulouse", "Occitanie"),
    ("Nantes", "Pays de la Loire"),
    ("Dakar", "Dakar"),
    ("Thiès", "Thiès"),
    ("Saint-Louis", "Saint-Louis"),
]


PAYMENT_METHODS = ["card", "cash", "mobile_money", "bank_transfer"]


def generate_products(number_of_products: int = 30) -> pd.DataFrame:
    products = []
    product_index = 1

    for category, product_names in PRODUCT_CATEGORIES.items():
        for product_name in product_names:
            if product_index > number_of_products:
                break

            unit_price = round(random.uniform(5, 1200), 2)
            stock_quantity = random.randint(20, 500)

            products.append(
                {
                    "product_id": f"P{product_index:03d}",
                    "product_name": product_name,
                    "category": category,
                    "unit_price": unit_price,
                    "stock_quantity": stock_quantity,
                }
            )

            product_index += 1

    return pd.DataFrame(products)


def generate_stores(number_of_stores: int = 10) -> pd.DataFrame:
    selected_cities = CITIES[:number_of_stores]

    stores = []

    for index, (city, region) in enumerate(selected_cities, start=1):
        stores.append(
            {
                "store_id": f"S{index:03d}",
                "store_name": f"{city} Store",
                "city": city,
                "region": region,
            }
        )

    return pd.DataFrame(stores)


def generate_sales(
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    number_of_sales: int = 1000,
) -> pd.DataFrame:
    sales = []

    start_date = datetime.now() - timedelta(days=30)

    for index in range(1, number_of_sales + 1):
        product = products_df.sample(1).iloc[0]
        store = stores_df.sample(1).iloc[0]

        quantity = random.randint(1, 5)
        unit_price = float(product["unit_price"])
        total_amount = round(quantity * unit_price, 2)

        sale_timestamp = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        sales.append(
            {
                "sale_id": f"BATCH-{index:06d}",
                "product_id": product["product_id"],
                "store_id": store["store_id"],
                "customer_id": f"CUST-{fake.random_number(digits=6, fix_len=True)}",
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "payment_method": random.choice(PAYMENT_METHODS),
                "sale_timestamp": sale_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    return pd.DataFrame(sales)


def main():
    print("Génération des données batch...")

    products_df = generate_products()
    stores_df = generate_stores()
    sales_df = generate_sales(products_df, stores_df)

    products_path = RAW_DATA_DIR / "products.csv"
    stores_path = RAW_DATA_DIR / "stores.csv"
    sales_path = RAW_DATA_DIR / "sales_batch.csv"

    products_df.to_csv(products_path, index=False)
    stores_df.to_csv(stores_path, index=False)
    sales_df.to_csv(sales_path, index=False)

    print("Données générées avec succès ")
    print(f"Produits : {len(products_df)} lignes -> {products_path}")
    print(f"Magasins : {len(stores_df)} lignes -> {stores_path}")
    print(f"Ventes batch : {len(sales_df)} lignes -> {sales_path}")


if __name__ == "__main__":
    main()