import os

import psycopg2
from dotenv import load_dotenv


def main():
    load_dotenv()

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    cursor = connection.cursor()

    cursor.execute("SELECT current_database(), current_user;")
    database_name, user_name = cursor.fetchone()

    print("Connexion réussie à PostgreSQL ")
    print(f"Base de données : {database_name}")
    print(f"Utilisateur : {user_name}")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()