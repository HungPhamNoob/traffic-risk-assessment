from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'dang',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def test_gcs_connection(**context):
    from google.cloud import storage
    client = storage.Client()
    buckets = [b.name for b in client.list_buckets()]
    logging.info(f"✅ GCS buckets: {buckets}")
    return {"buckets_count": len(buckets)}

def test_postgres_connection(**context):
    import psycopg2
    conn = psycopg2.connect(
        host="postgres",
        database="capstone_db",
        user="capstone",
        password="changeme"  # Lấy từ env
    )
    cur = conn.cursor()
    cur.execute("SELECT postgis_version();")
    version = cur.fetchone()[0]
    logging.info(f"✅ PostGIS version: {version}")
    return {"postgis_ok": True}

with DAG(
    'dag_smoke_test',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'smoke']
) as dag:
    
    test_gcs = PythonOperator(
        task_id='test_gcs',
        python_callable=test_gcs_connection,
        provide_context=True,
    )
    
    test_pg = PythonOperator(
        task_id='test_postgres',
        python_callable=test_postgres_connection,
        provide_context=True,
    )
    
    test_gcs >> test_pg