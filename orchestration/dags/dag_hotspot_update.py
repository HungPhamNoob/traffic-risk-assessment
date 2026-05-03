from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta

default_args = {
    "owner": "hieu",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def compute_hotspots(ti, **context):
    """Run Sedona spatial analysis to compute hotspots"""
    # Trigger Spark job with Sedona for hotspot detection
    # Output: list of hotspot coordinates + risk scores
    return {
        "hotspots": [
            {"lat": 21.0285, "lon": 105.8542, "risk": 0.85},
            # ... more hotspots
        ],
        "computed_at": context["ds"],
    }


def update_postgis(ti, **context):
    """Update PostGIS serving table with new hotspots"""
    hook = PostgresHook(postgres_conn_id="postgres_capstone")
    hotspots = ti.xcom_pull(task_ids="compute_hotspots")["hotspots"]

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Clear previous hotspots for today
            cur.execute(
                "DELETE FROM hotspots WHERE date = %s", 
                (context["ds"],)
            )

            # Insert new hotspots
            for h in hotspots:
                cur.execute(
                    """
                    INSERT INTO hotspots 
                    (lat, lon, risk_score, date, geom)
                    VALUES (%s, %s, %s, %s, ST_SetSRID
                    (ST_MakePoint(%s, %s), 4326))
                    """,
                    (h["lat"], h["lon"], h["risk"], 
                     context["ds"], h["lon"], h["lat"]),
                )

    return {"updated": len(hotspots)}


with DAG(
    "dag_hotspot_update",
    default_args=default_args,
    schedule_interval="0 */4 * * *",  # Every 4 hours
    catchup=False,
    tags=["spatial", "hotspot", "sedona"],
) as dag:

    compute = PythonOperator(
        task_id="compute_hotspots",
        python_callable=compute_hotspots,
        provide_context=True,
    )

    update = PythonOperator(
        task_id="update_postgis",
        python_callable=update_postgis,
        provide_context=True,
    )

    compute >> update
