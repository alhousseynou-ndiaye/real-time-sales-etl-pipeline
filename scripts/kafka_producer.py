import json
import os
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker


load_dotenv()

fake = Faker("fr_FR")

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales_stream")

EVENT_COUNT = int(os.getenv("STREAM_EVENT_COUNT", "100"))
DELAY_SECONDS = float(os.getenv("STREAM_DELAY_SECONDS", "1"))


PAYMENT_METHODS = ["card", "cash", "mobile_money", "bank_transfer"]


def delivery_report(error, message):
    if error is not None:
        print(f"Erreur d'envoi Kafka  : {error}")
    else:
        print(
            f"Message envoyé topic={message.topic()} "
            f"partition={message.partition()} offset={message.offset()}"
        )


def load_reference_data():
    products_path = RAW_DATA_DIR / "products.csv"
    stores_path = RAW_DATA_DIR / "stores.csv"

    if not products_path.exists() or not stores_path.exists():
        raise FileNotFoundError(
            "products.csv ou stores.csv introuvable. "
            "Lance d'abord python scripts/generate_batch_data.py"
        )

    products_df = pd.read_csv(products_path)
    stores_df = pd.read_csv(stores_path)

    return products_df, stores_df


def build_sale_event(products_df, stores_df):
    product = products_df.sample(1).iloc[0]
    store = stores_df.sample(1).iloc[0]

    quantity = random.randint(1, 5)
    unit_price = float(product["unit_price"])
    total_amount = round(quantity * unit_price, 2)

    return {
        "sale_id": f"STREAM-{uuid.uuid4().hex[:12].upper()}",
        "product_id": str(product["product_id"]),
        "store_id": str(store["store_id"]),
        "customer_id": f"CUST-{fake.random_number(digits=6, fix_len=True)}",
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "sale_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    print("Démarrage du producer Kafka...")
    print(f"Bootstrap servers : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic : {KAFKA_TOPIC}")
    print(f"Nombre d'événements : {EVENT_COUNT}")
    print(f"Délai entre événements : {DELAY_SECONDS} seconde(s)")

    products_df, stores_df = load_reference_data()

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "sales-stream-producer",
        }
    )

    for index in range(1, EVENT_COUNT + 1):
        event = build_sale_event(products_df, stores_df)

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event["sale_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)

        print(f"Événement {index}/{EVENT_COUNT} produit : {event}")

        time.sleep(DELAY_SECONDS)

    producer.flush()

    print("Production Kafka terminée ")


if __name__ == "__main__":
    main()