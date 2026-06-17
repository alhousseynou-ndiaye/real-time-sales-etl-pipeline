# SETUP Guide

This guide explains how to run the Real-Time Sales & Inventory Analytics Pipeline from scratch.

---

## 1. Prerequisites

Make sure the following tools are installed:

- Python 3.11 or later
- Docker Desktop
- Git
- Java installed for PySpark
- Visual Studio Code or another code editor

To check Java:

```powershell
java -version
```

If Java is installed, the command should display a Java version.

---

## 2. Go to the Project Folder

```powershell
cd C:\Users\DYNABOOK\etl-final-project
```

---

## 3. Activate the Python Virtual Environment

```powershell
.\.venv\Scripts\Activate.ps1
```

You should see:

```txt
(.venv) PS C:\Users\DYNABOOK\etl-final-project>
```

If PowerShell blocks the activation script, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

Then activate again:

```powershell
.\.venv\Scripts\Activate.ps1
```

---

## 4. Install Python Dependencies

If dependencies are not installed yet, run:

```powershell
pip install -r requirements.txt
```

Important dependencies include:

```txt
pandas
psycopg2-binary
python-dotenv
faker
sqlalchemy
confluent-kafka
streamlit
plotly
pyspark
```

---

## 5. Environment Variables

The project uses a `.env` file.

Example:

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

---

## 6. Start Docker Containers

Open Docker Desktop first.

Then run:

```powershell
docker compose up -d
```

Check that the containers are running:

```powershell
docker ps
```

You should see containers such as:

```txt
etl_postgres
etl_redpanda
etl_airflow-webserver
etl_airflow-scheduler
```

---

## 7. Create Kafka Topic

Create the Kafka topic if it does not already exist:

```powershell
docker exec -it etl_redpanda rpk topic create sales_stream
```

Verify the topic:

```powershell
docker exec -it etl_redpanda rpk topic list
```

You should see:

```txt
sales_stream
```

If the topic already exists, this is not a problem.

---

## 8. Test PostgreSQL Connection

Run:

```powershell
python scripts/test_db_connection.py
```

If the connection is successful, the script should print database information.

---

## 9. Run the Full Batch Pipeline with PySpark

Run:

```powershell
python scripts/run_batch_pipeline.py
```

This command executes:

```txt
1. Generate synthetic CSV data
2. Load raw CSV data into PostgreSQL
3. Run PySpark transformations
4. Load staging and analytics tables into PostgreSQL
5. Print final table counts
```

Expected result:

```txt
Batch pipeline completed successfully with PySpark.
```

Example final counts:

```txt
raw.products: 25
raw.stores: 10
raw.sales_batch: 1000
staging.sales_clean: 1000
analytics.daily_sales_summary: around 300
analytics.product_performance: 25
```

Windows may display Spark warnings such as:

```txt
Did not find winutils.exe
Unable to load native-hadoop library
```

These warnings are not blocking in this local setup if the Spark job finishes successfully.

---

## 10. Run Only the PySpark Transformations

If the CSV files already exist and raw data is already loaded, you can run only the Spark transformation step:

```powershell
python scripts/spark_transformations.py
```

This script:

```txt
reads products.csv, stores.csv and sales_batch.csv
joins sales with products and stores
creates staging.sales_clean
creates analytics.daily_sales_summary
creates analytics.product_performance
loads results into PostgreSQL
```

Expected result:

```txt
Spark transformations loaded into PostgreSQL successfully.
```

---

## 11. Validate PostgreSQL Data

Open PostgreSQL inside the container:

```powershell
docker exec -it etl_postgres psql -U etl_user -d etl_sales_db
```

Run:

```sql
SELECT COUNT(*) FROM raw.products;
SELECT COUNT(*) FROM raw.stores;
SELECT COUNT(*) FROM raw.sales_batch;
SELECT COUNT(*) FROM staging.sales_clean;
SELECT COUNT(*) FROM analytics.daily_sales_summary;
SELECT COUNT(*) FROM analytics.product_performance;
```

Exit PostgreSQL:

```sql
\q
```

You can also validate the schema:

```powershell
docker exec -it etl_postgres psql -U etl_user -d etl_sales_db -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='analytics' AND table_name='daily_sales_summary' ORDER BY ordinal_position;"
```

---

## 12. Launch Airflow

Airflow is started with Docker Compose.

Airflow URL:

```txt
http://localhost:8080
```

Credentials:

```txt
username: admin
password: admin
```

In Airflow:

```txt
DAGs
→ sales_batch_etl_pipeline
→ Trigger DAG
```

The DAG should show successful tasks in green.

To restart Airflow:

```powershell
docker compose restart airflow-webserver airflow-scheduler
```

To view logs:

```powershell
docker compose logs -f airflow-webserver
```

Scheduler logs:

```powershell
docker compose logs -f airflow-scheduler
```

---

## 13. Run the Streaming Pipeline

The streaming pipeline needs two terminals.

---

### Terminal 1 — Kafka Consumer

```powershell
cd C:\Users\DYNABOOK\etl-final-project
.\.venv\Scripts\Activate.ps1
python scripts/kafka_consumer.py
```

Leave this terminal open.

The consumer waits for events from the Kafka topic.

---

### Terminal 2 — Kafka Producer

```powershell
cd C:\Users\DYNABOOK\etl-final-project
.\.venv\Scripts\Activate.ps1
python scripts/kafka_producer.py
```

The producer sends simulated live sales events to the Redpanda topic.

---

## 14. Validate Real-Time Metrics

Open PostgreSQL:

```powershell
docker exec -it etl_postgres psql -U etl_user -d etl_sales_db
```

Run:

```sql
SELECT *
FROM analytics.realtime_sales_metrics
ORDER BY metric_timestamp DESC
LIMIT 5;
```

You can also check the latest streaming sales:

```sql
SELECT *
FROM raw.sales_stream
ORDER BY sale_timestamp DESC
LIMIT 5;
```

Exit:

```sql
\q
```

---

## 15. Launch Streamlit Dashboard

Run:

```powershell
streamlit run dashboard/app.py
```

Open:

```txt
http://localhost:8501
```

The dashboard displays:

```txt
Total revenue
Number of orders
Items sold
Average basket
Revenue by day
Orders by day
Revenue by city
Top products
Product categories
Real-time Kafka metrics
Latest streaming sales
```

---

## 16. Full Local Demo Order

For a complete demo from scratch:

```powershell
cd C:\Users\DYNABOOK\etl-final-project
.\.venv\Scripts\Activate.ps1
docker compose up -d
docker ps
docker exec -it etl_redpanda rpk topic create sales_stream
python scripts/test_db_connection.py
python scripts/run_batch_pipeline.py
streamlit run dashboard/app.py
```

For streaming, use two extra terminals:

Terminal 1:

```powershell
python scripts/kafka_consumer.py
```

Terminal 2:

```powershell
python scripts/kafka_producer.py
```

---

## 17. Stop the Project

To stop all containers:

```powershell
docker compose down
```

To stop and delete volumes:

```powershell
docker compose down -v
```

Warning:

```txt
docker compose down -v
```

deletes the PostgreSQL data volume.

Use it only if you want to reset the database.

---

## 18. Common Issues

### PowerShell blocks venv activation

Run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

Then:

```powershell
.\.venv\Scripts\Activate.ps1
```

---

### Kafka topic already exists

If this command:

```powershell
docker exec -it etl_redpanda rpk topic create sales_stream
```

returns that the topic already exists, it is fine.

Continue with the next step.

---

### Spark warning on Windows

You may see:

```txt
Did not find winutils.exe
Unable to load native-hadoop library
```

In this local setup, these warnings are not blocking if the job finishes with:

```txt
Spark transformations loaded into PostgreSQL successfully.
```

---

### Streamlit does not refresh

Use the refresh button in the dashboard or restart:

```powershell
streamlit run dashboard/app.py
```

---

### Airflow page does not load

Restart Airflow:

```powershell
docker compose restart airflow-webserver airflow-scheduler
```

Then open:

```txt
http://localhost:8080
```

---

## 19. Final Checklist

Before the presentation, verify:

```txt
Docker containers are running
PostgreSQL connection works
Batch pipeline works with PySpark
Airflow UI is accessible
Kafka topic exists
Producer and consumer work
Realtime metrics are inserted
Streamlit dashboard opens
GitHub repository is up to date
README and SETUP are updated
Presentation PDF is available
```

---

## 20. Main Commands Summary

```powershell
cd C:\Users\DYNABOOK\etl-final-project
.\.venv\Scripts\Activate.ps1
docker compose up -d
python scripts/run_batch_pipeline.py
streamlit run dashboard/app.py
```

Streaming:

```powershell
python scripts/kafka_consumer.py
```

Then, in another terminal:

```powershell
python scripts/kafka_producer.py
```