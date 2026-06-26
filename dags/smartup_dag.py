"""
QADAM 4: AIRFLOW DAG — smartup_etl.py'ni har kuni avtomatik ishga tushiradi

Har bir endpoint uchun ALOHIDA PythonOperator Task yaratilgan.
Ular bir-biriga BOG'LIQ EMAS — CeleryExecutor ularni PARALLEL ishlatadi.
Airflow UI Grid/Graph ko'rinishida 18 ta alohida quti ko'rinadi.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/dags")
from smartup_etl import ENDPOINTS, etl_one_endpoint  # noqa: E402

default_args = {
    "owner": "hosilbek",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="smartup_to_postgres",
    default_args=default_args,
    description="Smartup ERP -> smartup_v2 bazasiga har kuni sync (18 parallel task)",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["smartup", "etl"],
) as dag:

    # Har bir endpoint uchun alohida Task — bogʻliqlik yoʻq, parallel ishlaydi
    for _table_name, (_url, _need_date) in ENDPOINTS.items():
        PythonOperator(
            task_id=f"sync_{_table_name}",
            python_callable=etl_one_endpoint,
            op_kwargs={
                "table_name": _table_name,
                "url": _url,
                "need_date": _need_date,
            },
        )
