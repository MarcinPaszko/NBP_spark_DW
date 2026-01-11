from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello_world():
    print("Airflow works!")

with DAG(
    dag_id='01_test_connection',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None, # Uruchamiamy tylko recznie dla testu
    catchup=False
) as dag:

    task_test = PythonOperator(
        task_id='check_airflow',
        python_callable=hello_world
    )