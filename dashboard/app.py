import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


st.set_page_config(
    page_title="Real-Time Sales Analytics",
    page_icon="📊",
    layout="wide",
)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        dbname=os.getenv("POSTGRES_DB", "etl_sales_db"),
        user=os.getenv("POSTGRES_USER", "etl_user"),
        password=os.getenv("POSTGRES_PASSWORD", "etl_password"),
    )


@st.cache_data(ttl=10)
def run_query(query: str) -> pd.DataFrame:
    connection = get_connection()
    try:
        return pd.read_sql_query(query, connection)
    finally:
        connection.close()


def load_batch_kpis() -> pd.DataFrame:
    query = """
        SELECT
            COALESCE(SUM(total_revenue), 0) AS total_revenue,
            COALESCE(SUM(total_orders), 0) AS total_orders,
            COALESCE(SUM(total_items_sold), 0) AS total_items_sold,
            COALESCE(ROUND(SUM(total_revenue) / NULLIF(SUM(total_orders), 0), 2), 0) AS average_basket
        FROM analytics.daily_sales_summary;
    """
    return run_query(query)


def load_daily_sales() -> pd.DataFrame:
    query = """
        SELECT
            sale_date,
            SUM(total_revenue) AS total_revenue,
            SUM(total_orders) AS total_orders,
            SUM(total_items_sold) AS total_items_sold,
            ROUND(SUM(total_revenue) / NULLIF(SUM(total_orders), 0), 2) AS average_basket
        FROM analytics.daily_sales_summary
        GROUP BY sale_date
        ORDER BY sale_date;
    """
    return run_query(query)


def load_city_sales() -> pd.DataFrame:
    query = """
        SELECT
            city,
            SUM(total_revenue) AS total_revenue,
            SUM(total_orders) AS total_orders
        FROM analytics.daily_sales_summary
        GROUP BY city
        ORDER BY total_revenue DESC;
    """
    return run_query(query)


def load_top_products() -> pd.DataFrame:
    query = """
        SELECT
            product_name,
            category,
            total_revenue,
            total_quantity_sold,
            number_of_orders
        FROM analytics.product_performance
        ORDER BY total_revenue DESC
        LIMIT 10;
    """
    return run_query(query)


def load_category_sales() -> pd.DataFrame:
    query = """
        SELECT
            category,
            SUM(total_revenue) AS total_revenue,
            SUM(total_quantity_sold) AS total_quantity_sold
        FROM analytics.product_performance
        GROUP BY category
        ORDER BY total_revenue DESC;
    """
    return run_query(query)


def load_realtime_metrics() -> pd.DataFrame:
    query = """
        SELECT
            metric_timestamp,
            total_revenue,
            total_orders,
            average_basket
        FROM analytics.realtime_sales_metrics
        ORDER BY metric_timestamp DESC
        LIMIT 50;
    """
    return run_query(query)


def load_recent_stream_sales() -> pd.DataFrame:
    query = """
        SELECT
            ss.sale_id,
            ss.sale_timestamp,
            p.product_name,
            p.category,
            s.store_name,
            s.city,
            ss.quantity,
            ss.total_amount,
            ss.payment_method
        FROM raw.sales_stream ss
        LEFT JOIN raw.products p
            ON ss.product_id = p.product_id
        LEFT JOIN raw.stores s
            ON ss.store_id = s.store_id
        ORDER BY ss.ingestion_timestamp DESC
        LIMIT 20;
    """
    return run_query(query)



st.title("Real-Time Sales & Inventory Analytics Pipeline")

st.markdown(
    """
    Dashboard de suivi des ventes batch et temps réel.  
    Les données batch sont orchestrées avec Airflow, et les ventes temps réel passent par Kafka / Redpanda.
    """
)

if st.button("🔄 Rafraîchir maintenant"):
    st.cache_data.clear()
    st.rerun()


batch_kpis = load_batch_kpis()
daily_sales = load_daily_sales()
city_sales = load_city_sales()
top_products = load_top_products()
category_sales = load_category_sales()
realtime_metrics = load_realtime_metrics()
recent_stream_sales = load_recent_stream_sales()


st.subheader("Vue globale batch")

if not batch_kpis.empty:
    kpis = batch_kpis.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Chiffre d'affaires total",
        f"{kpis['total_revenue']:,.2f} €",
    )

    col2.metric(
        "Nombre de commandes",
        f"{int(kpis['total_orders']):,}",
    )

    col3.metric(
        "Articles vendus",
        f"{int(kpis['total_items_sold']):,}",
    )

    col4.metric(
        "Panier moyen",
        f"{kpis['average_basket']:,.2f} €",
    )


st.divider()


st.subheader("Analyse des ventes historiques")

col_left, col_right = st.columns(2)

with col_left:
    if not daily_sales.empty:
        fig_daily_revenue = px.line(
            daily_sales,
            x="sale_date",
            y="total_revenue",
            markers=True,
            title="Évolution du chiffre d'affaires par jour",
            labels={
                "sale_date": "Date",
                "total_revenue": "Chiffre d'affaires (€)",
            },
        )
        st.plotly_chart(fig_daily_revenue, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible pour les ventes par jour.")

with col_right:
    if not daily_sales.empty:
        fig_daily_orders = px.bar(
            daily_sales,
            x="sale_date",
            y="total_orders",
            title="Nombre de commandes par jour",
            labels={
                "sale_date": "Date",
                "total_orders": "Commandes",
            },
        )
        st.plotly_chart(fig_daily_orders, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible pour les commandes par jour.")


col_left, col_right = st.columns(2)

with col_left:
    if not city_sales.empty:
        fig_city = px.bar(
            city_sales,
            x="city",
            y="total_revenue",
            title="Chiffre d'affaires par ville",
            labels={
                "city": "Ville",
                "total_revenue": "Chiffre d'affaires (€)",
            },
        )
        st.plotly_chart(fig_city, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible pour les villes.")

with col_right:
    if not top_products.empty:
        fig_top_products = px.bar(
            top_products,
            x="total_revenue",
            y="product_name",
            orientation="h",
            title="Top 10 produits par chiffre d'affaires",
            labels={
                "product_name": "Produit",
                "total_revenue": "Chiffre d'affaires (€)",
            },
        )
        fig_top_products.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_top_products, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible pour les produits.")


col_left, col_right = st.columns(2)

with col_left:
    if not category_sales.empty:
        fig_category = px.pie(
            category_sales,
            names="category",
            values="total_revenue",
            title="Répartition du chiffre d'affaires par catégorie",
        )
        st.plotly_chart(fig_category, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible pour les catégories.")

with col_right:
    if not top_products.empty:
        fig_quantity = px.bar(
            top_products,
            x="product_name",
            y="total_quantity_sold",
            title="Quantités vendues par produit",
            labels={
                "product_name": "Produit",
                "total_quantity_sold": "Quantité vendue",
            },
        )
        st.plotly_chart(fig_quantity, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible pour les quantités.")


st.divider()


st.subheader("Flux temps réel Kafka / Redpanda")

if not realtime_metrics.empty:
    latest_metrics = realtime_metrics.iloc[0]

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "CA temps réel récent",
        f"{latest_metrics['total_revenue']:,.2f} €",
    )

    col2.metric(
        "Ventes temps réel",
        f"{int(latest_metrics['total_orders']):,}",
    )

    col3.metric(
        "Panier moyen temps réel",
        f"{latest_metrics['average_basket']:,.2f} €",
    )

    realtime_sorted = realtime_metrics.sort_values("metric_timestamp")

    fig_realtime_revenue = px.line(
        realtime_sorted,
        x="metric_timestamp",
        y="total_revenue",
        markers=True,
        title="Évolution du chiffre d'affaires temps réel",
        labels={
            "metric_timestamp": "Timestamp",
            "total_revenue": "Chiffre d'affaires temps réel (€)",
        },
    )
    st.plotly_chart(fig_realtime_revenue, use_container_width=True)

    fig_realtime_orders = px.line(
        realtime_sorted,
        x="metric_timestamp",
        y="total_orders",
        markers=True,
        title="Évolution du nombre de ventes temps réel",
        labels={
            "metric_timestamp": "Timestamp",
            "total_orders": "Nombre de ventes",
        },
    )
    st.plotly_chart(fig_realtime_orders, use_container_width=True)

else:
    st.info(
        "Aucune métrique temps réel pour le moment. "
        "Lance kafka_consumer.py puis kafka_producer.py pour alimenter le flux."
    )


st.subheader("Dernières ventes streaming")

if not recent_stream_sales.empty:
    st.dataframe(recent_stream_sales, use_container_width=True)
else:
    st.info("Aucune vente streaming trouvée pour le moment.")


st.divider()

st.caption(
    "Pipeline: Batch CSV → PostgreSQL raw → staging → analytics via Airflow | "
    "Streaming: Producer → Kafka/Redpanda → Consumer → PostgreSQL"
)