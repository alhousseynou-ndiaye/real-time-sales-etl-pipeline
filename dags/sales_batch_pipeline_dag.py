from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "alhousseynou",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="sales_batch_etl_pipeline",
    description="Batch ETL/ELT pipeline for retail sales analytics",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "sales", "postgres", "analytics"],
) as dag:

    generate_batch_data = BashOperator(
        task_id="generate_batch_data",
        bash_command="python /opt/airflow/scripts/generate_batch_data.py",
    )

    load_batch_to_postgres = BashOperator(
        task_id="load_batch_to_postgres",
        bash_command="python /opt/airflow/scripts/load_batch_to_postgres.py",
    )

    transform_raw_to_staging = BashOperator(
        task_id="transform_raw_to_staging",
        bash_command=(
            "python - <<'PY'\n"
            "import os\n"
            "from pathlib import Path\n"
            "import psycopg2\n"
            "\n"
            "sql_path = Path('/opt/airflow/sql/02_staging_to_clean.sql')\n"
            "sql_content = sql_path.read_text(encoding='utf-8')\n"
            "\n"
            "connection = psycopg2.connect(\n"
            "    host=os.getenv('POSTGRES_HOST', 'postgres'),\n"
            "    port=os.getenv('POSTGRES_PORT', '5432'),\n"
            "    dbname=os.getenv('POSTGRES_DB', 'etl_sales_db'),\n"
            "    user=os.getenv('POSTGRES_USER', 'etl_user'),\n"
            "    password=os.getenv('POSTGRES_PASSWORD', 'etl_password'),\n"
            ")\n"
            "\n"
            "try:\n"
            "    with connection.cursor() as cursor:\n"
            "        cursor.execute(sql_content)\n"
            "    connection.commit()\n"
            "    print('Transformation raw -> staging terminée avec succès')\n"
            "except Exception:\n"
            "    connection.rollback()\n"
            "    raise\n"
            "finally:\n"
            "    connection.close()\n"
            "PY"
        ),
    )

    build_analytics = BashOperator(
        task_id="build_analytics",
        bash_command=(
            "python - <<'PY'\n"
            "import os\n"
            "from pathlib import Path\n"
            "import psycopg2\n"
            "\n"
            "sql_path = Path('/opt/airflow/sql/03_build_analytics.sql')\n"
            "sql_content = sql_path.read_text(encoding='utf-8')\n"
            "\n"
            "connection = psycopg2.connect(\n"
            "    host=os.getenv('POSTGRES_HOST', 'postgres'),\n"
            "    port=os.getenv('POSTGRES_PORT', '5432'),\n"
            "    dbname=os.getenv('POSTGRES_DB', 'etl_sales_db'),\n"
            "    user=os.getenv('POSTGRES_USER', 'etl_user'),\n"
            "    password=os.getenv('POSTGRES_PASSWORD', 'etl_password'),\n"
            ")\n"
            "\n"
            "try:\n"
            "    with connection.cursor() as cursor:\n"
            "        cursor.execute(sql_content)\n"
            "    connection.commit()\n"
            "    print('Tables analytics construites avec succès')\n"
            "except Exception:\n"
            "    connection.rollback()\n"
            "    raise\n"
            "finally:\n"
            "    connection.close()\n"
            "PY"
        ),
    )

    (
        generate_batch_data
        >> load_batch_to_postgres
        >> transform_raw_to_staging
        >> build_analytics
    )