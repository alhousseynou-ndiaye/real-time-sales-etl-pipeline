# Architecture Diagram — Real-Time Sales & Inventory Analytics Pipeline

```mermaid
flowchart LR

    subgraph Batch["Batch ETL / ELT Pipeline"]
        A["Synthetic CSV Data<br/>products.csv<br/>stores.csv<br/>sales_batch.csv"]
        B["Python ETL Scripts<br/>generate_batch_data.py<br/>load_batch_to_postgres.py"]
        C["PostgreSQL RAW Layer<br/>raw.products<br/>raw.stores<br/>raw.sales_batch"]
        D["SQL Transformation<br/>02_staging_to_clean.sql"]
        E["PostgreSQL STAGING Layer<br/>staging.sales_clean"]
        F["SQL Analytics Models<br/>03_build_analytics.sql"]
        G["PostgreSQL ANALYTICS Layer<br/>daily_sales_summary<br/>product_performance"]
    end

    subgraph Orchestration["Orchestration"]
        H["Apache Airflow DAG<br/>sales_batch_etl_pipeline"]
    end

    subgraph Streaming["Real-Time Streaming Pipeline"]
        I["Kafka Producer<br/>kafka_producer.py"]
        J["Redpanda / Kafka Broker<br/>Topic: sales_stream"]
        K["Kafka Consumer<br/>kafka_consumer.py"]
        L["PostgreSQL Streaming RAW<br/>raw.sales_stream"]
        M["Real-Time Metrics<br/>analytics.realtime_sales_metrics"]
    end

    subgraph Dashboard["Visualization"]
        N["Streamlit Dashboard<br/>dashboard/app.py"]
    end

    H --> B
    B --> A
    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> N

    I --> J
    J --> K
    K --> L
    K --> M
    L --> N
    M --> N