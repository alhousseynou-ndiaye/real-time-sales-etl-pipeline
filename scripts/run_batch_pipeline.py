import os
import subprocess
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
SQL_DIR = BASE_DIR / "sql"


def run_python_script(script_name: str) -> None:
    script_path = SCRIPTS_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script introuvable : {script_path}")

    print(f"\n▶ Exécution du script Python : {script_name}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=BASE_DIR,
        text=True,
        capture_output=True,
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Erreur pendant l'exécution de {script_name}")

    print(f" Script terminé : {script_name}")


def get_connection():
    load_dotenv(BASE_DIR / ".env")

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def run_sql_file(sql_file_name: str) -> None:
    sql_path = SQL_DIR / sql_file_name

    if not sql_path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_path}")

    print(f"\n▶ Exécution du fichier SQL : {sql_file_name}")

    sql_content = sql_path.read_text(encoding="utf-8")

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_content)

        connection.commit()
        print(f" SQL terminé : {sql_file_name}")

    except Exception as error:
        connection.rollback()
        print(f" Erreur SQL dans : {sql_file_name}")
        raise error

    finally:
        connection.close()


def print_pipeline_summary() -> None:
    print("\n▶ Vérification des tables finales")

    connection = get_connection()

    queries = {
        "raw.products": "SELECT COUNT(*) FROM raw.products;",
        "raw.stores": "SELECT COUNT(*) FROM raw.stores;",
        "raw.sales_batch": "SELECT COUNT(*) FROM raw.sales_batch;",
        "staging.sales_clean": "SELECT COUNT(*) FROM staging.sales_clean;",
        "analytics.daily_sales_summary": "SELECT COUNT(*) FROM analytics.daily_sales_summary;",
        "analytics.product_performance": "SELECT COUNT(*) FROM analytics.product_performance;",
    }

    try:
        with connection.cursor() as cursor:
            for table_name, query in queries.items():
                cursor.execute(query)
                count = cursor.fetchone()[0]
                print(f"{table_name:<40} {count} lignes")

    finally:
        connection.close()


def main() -> None:
    print("==============================================")
    print("Lancement du pipeline batch ETL/ELT")
    print("==============================================")

    run_python_script("generate_batch_data.py")
    run_python_script("load_batch_to_postgres.py")
    run_sql_file("02_staging_to_clean.sql")
    run_sql_file("03_build_analytics.sql")
    print_pipeline_summary()

    print("\n==============================================")
    print("Pipeline batch terminé avec succès ")
    print("==============================================")


if __name__ == "__main__":
    main()