"""
DEMO DAG — smartup_etl_demo.py'ni qo'lda ishga tushirish uchun

Asosiy DAG'dan farqi:
  - smartup_etl_demo (so'nggi 30 kun ma'lumoti) dan import qiladi
  - schedule=None (faqat qo'lda trigger qilinadi)
  - dag_id="smartup_to_postgres_demo"

Har bir endpoint uchun ALOHIDA Task — parallel ishlaydi.
Airflow UI da 18 ta quti ko'rinadi, bir nechtasi bir vaqtda "running".
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/dags")
from smartup_etl_demo import ENDPOINTS, etl_one_endpoint  # noqa: E402

default_args = {
    "owner": "hosilbek",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="smartup_to_postgres_demo",
    default_args=default_args,
    description="DEMO: Smartup ERP -> smartup_v2 (so'nggi 30 kun, 18 parallel task)",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["smartup", "etl", "demo"],
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
