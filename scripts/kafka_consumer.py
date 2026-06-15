import json
import os

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales_stream")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sales-stream-consumer-group")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        dbname=os.getenv("POSTGRES_DB", "etl_sales_db"),
        user=os.getenv("POSTGRES_USER", "etl_user"),
        password=os.getenv("POSTGRES_PASSWORD", "etl_password"),
    )


def insert_sale_event(connection, event):
    query = """
        INSERT INTO raw.sales_stream (
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
        VALUES (
            %(sale_id)s,
            %(product_id)s,
            %(store_id)s,
            %(customer_id)s,
            %(quantity)s,
            %(unit_price)s,
            %(total_amount)s,
            %(payment_method)s,
            %(sale_timestamp)s
        )
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
        cursor.execute(query, event)


def update_realtime_metrics(connection):
    query = """
        INSERT INTO analytics.realtime_sales_metrics (
            total_revenue,
            total_orders,
            average_basket
        )
        SELECT
            COALESCE(ROUND(SUM(total_amount), 2), 0) AS total_revenue,
            COUNT(*) AS total_orders,
            COALESCE(ROUND(AVG(total_amount), 2), 0) AS average_basket
        FROM raw.sales_stream
        WHERE ingestion_timestamp >= NOW() - INTERVAL '5 minutes';
    """

    with connection.cursor() as cursor:
        cursor.execute(query)


def main():
    print("Démarrage du consumer Kafka...")
    print(f"Bootstrap servers : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic : {KAFKA_TOPIC}")
    print(f"Group ID : {KAFKA_GROUP_ID}")

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([KAFKA_TOPIC])

    connection = get_connection()

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                print(f"Erreur Kafka  : {message.error()}")
                continue

            event = json.loads(message.value().decode("utf-8"))

            insert_sale_event(connection, event)
            update_realtime_metrics(connection)

            connection.commit()
            consumer.commit(message)

            print(
                "Vente consommée et insérée  "
                f"sale_id={event['sale_id']} "
                f"amount={event['total_amount']}"
            )

    except KeyboardInterrupt:
        print("Arrêt manuel du consumer.")

    except Exception as error:
        connection.rollback()
        print("Erreur pendant la consommation ")
        raise error

    finally:
        consumer.close()
        connection.close()
        print("Consumer arrêté.")


if __name__ == "__main__":
    main()