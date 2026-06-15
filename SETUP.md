# SETUP.md — Real-Time Sales & Inventory Analytics Pipeline

## 1. Prerequisites

Before running the project, make sure the following tools are installed:

- Docker Desktop
- Python 3.12+
- Git
- Visual Studio Code or another code editor

> The project was developed and tested on **Windows** using **PowerShell**.

---

## 2. Clone or Open the Project

```powershell
cd C:\Users\DYNABOOK\etl-final-project
```

---

## 3. Create the Python Virtual Environment

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks the activation:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

---

## 4. Install Python Dependencies

```powershell
pip install -r requirements.txt
```

Main libraries: `pandas`, `psycopg2-binary`, `python-dotenv`, `faker`, `sqlalchemy`, `confluent-kafka`, `streamlit`, `plotly`

---

## 5. Environment Variables

Create a `.env` file at the root of the project:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=etl_sales_db
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_password

DATABASE_URL=postgresql://etl_user:etl_password@localhost:5433/etl_sales_db

KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_TOPIC=sales_stream
KAFKA_GROUP_ID=sales-stream-consumer-group

STREAM_EVENT_COUNT=100
STREAM_DELAY_SECONDS=1
```

> **Note:**
> - From the **host machine** → PostgreSQL on `localhost:5433`, Kafka on `localhost:19092`
> - **Inside Docker containers** → PostgreSQL on `postgres:5432`

---

## 6. Start Docker Services

```powershell
docker compose up -d
docker ps
```

Expected containers:

| Container | Status |
|---|---|
| `etl_postgres` | running |
| `etl_redpanda` | running |
| `etl_airflow_webserver` | running |
| `etl_airflow_scheduler` | running |
| `etl_airflow_init` | exited ✅ (normal) |

---

## 7. Create the Kafka Topic

```powershell
docker exec -it etl_redpanda rpk topic create sales_stream
```

Verify:

```powershell
docker exec -it etl_redpanda rpk topic list
# Expected: sales_stream
```

> If the topic already exists, that's fine — continue.

---

## 8. Test PostgreSQL Connection

```powershell
python scripts\test_db_connection.py
```

---

## 9. Run the Batch Pipeline

```powershell
python scripts\run_batch_pipeline.py
```

This runs the full batch flow:

```text
generate_batch_data.py
        ↓
load_batch_to_postgres.py
        ↓
02_staging_to_clean.sql
        ↓
03_build_analytics.sql
```

Expected output:

| Table | Rows |
|---|---|
| `raw.products` | 25 |
| `raw.stores` | 10 |
| `raw.sales_batch` | 1 000 |
| `staging.sales_clean` | 1 000 |
| `analytics.daily_sales_summary` | ~290–300 |
| `analytics.product_performance` | 25 |

---

## 10. Validate PostgreSQL Tables

```powershell
docker exec -it etl_postgres psql -U etl_user -d etl_sales_db
```

```sql
SELECT COUNT(*) FROM raw.products;
SELECT COUNT(*) FROM raw.stores;
SELECT COUNT(*) FROM raw.sales_batch;
SELECT COUNT(*) FROM staging.sales_clean;
SELECT COUNT(*) FROM analytics.daily_sales_summary;
SELECT COUNT(*) FROM analytics.product_performance;
\q
```

---

## 11. Run Airflow

Open **http://localhost:8080**
username: admin
password: admin

DAG: `sales_batch_etl_pipeline`

```text
generate_batch_data → load_batch_to_postgres → transform_raw_to_staging → build_analytics
```

Steps: activate the DAG → click **Trigger DAG** → wait until all tasks are green.

---

## 12. Run the Streaming Pipeline

Open **two terminals**, both with the virtual environment activated.

**Terminal 1 — Consumer:**

```powershell
cd C:\Users\DYNABOOK\etl-final-project
.\.venv\Scripts\Activate.ps1
python scripts\kafka_consumer.py
```

**Terminal 2 — Producer:**

```powershell
cd C:\Users\DYNABOOK\etl-final-project
.\.venv\Scripts\Activate.ps1
python scripts\kafka_producer.py
```

Expected flow:

```text
Producer → sales_stream topic → Consumer → raw.sales_stream → analytics.realtime_sales_metrics
```

---

## 13. Validate Streaming Data

```powershell
docker exec -it etl_postgres psql -U etl_user -d etl_sales_db
```

```sql
-- Row counts
SELECT COUNT(*) FROM raw.sales_stream;
SELECT COUNT(*) FROM analytics.realtime_sales_metrics;

-- Latest streaming sales
SELECT sale_id, product_id, store_id, quantity, total_amount, payment_method, sale_timestamp
FROM raw.sales_stream
ORDER BY ingestion_timestamp DESC
LIMIT 10;

-- Real-time metrics
SELECT metric_timestamp, total_revenue, total_orders, average_basket
FROM analytics.realtime_sales_metrics
ORDER BY metric_timestamp DESC
LIMIT 10;

\q
```

---

## 14. Run the Streamlit Dashboard

```powershell
streamlit run dashboard\app.py
```

Open **http://localhost:8501**

The dashboard shows: total revenue, orders, items sold, average basket, revenue by day and city, top products, revenue by category, real-time Kafka metrics and latest streaming sales.

To refresh data manually, click **Rafraîchir maintenant**.

---

## 15. Stop the Project

Stop containers (keeps PostgreSQL data):

```powershell
docker compose down
```

Stop containers and delete all data:

```powershell
docker compose down -v
```

> ⚠️ `docker compose down -v` permanently deletes the PostgreSQL data volume.

---

## 16. Common Issues

### Airflow admin login does not work

```powershell
docker exec -it etl_airflow_webserver airflow users create \
  --username admin --password admin \
  --firstname Alhousseynou --lastname Ndiaye \
  --role Admin --email admin@example.com
```

To reset an existing user:

```powershell
docker exec -it etl_airflow_webserver airflow users reset-password --username admin --password admin
```

### PowerShell does not support `< sql/file.sql`

```powershell
Get-Content .\sql\02_staging_to_clean.sql | docker exec -i etl_postgres psql -U etl_user -d etl_sales_db
```

### Redpanda topic already exists

Not an error — continue normally.

### Dashboard does not show real-time data

Start the consumer first, then the producer, then click the refresh button in Streamlit.

---

## 17. Demo Checklist

- [ ] Docker containers are running (`docker ps`)
- [ ] PostgreSQL tables contain data
- [ ] Airflow DAG is visible and all tasks succeeded
- [ ] Kafka producer sends messages
- [ ] Kafka consumer inserts messages into PostgreSQL
- [ ] Streamlit dashboard shows batch and real-time metrics

**Quick commands:**

```powershell
docker ps
python scripts\run_batch_pipeline.py
python scripts\kafka_consumer.py
python scripts\kafka_producer.py
streamlit run dashboard\app.py
```