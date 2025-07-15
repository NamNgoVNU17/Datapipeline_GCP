from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.insert(0, os.path.abspath("/opt/airflow"))
from src.fetch_exchange_rates import fetch_exchange_rates
from src.upload_gcs import upload_to_gcs
from config.config import FILE_SAVE_CSV, BUCKET_NAME, BLOB_PATH

default_args = {
    'owner': 'nam',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='fetch_exchange_rates_and_upload',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_exchange_rates',
        python_callable=fetch_exchange_rates,
        do_xcom_push=False
    )

    upload_data = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'file_path': FILE_SAVE_CSV,
            'bucket_name': BUCKET_NAME,
            'blob_path': BLOB_PATH
        },
        do_xcom_push = False
    )

    fetch_data >> upload_data
