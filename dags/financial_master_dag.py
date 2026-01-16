from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 2,                          
    'retry_delay': timedelta(minutes=5),   
}

with DAG(
    'financial_system_pipeline',
    default_args=default_args,
    description='Automatyczny pobór NBP i Stocks z obsługa bledów',
    schedule_interval='0 18 * * *',        
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    
    task_nbp = BashOperator(
        task_id='nbp_data_ingest',
        bash_command='python3 /opt/airflow/scripts/fetch_nbp.py'
    )

    
    task_stocks = BashOperator(
        task_id='stocks_data_ingest',
        bash_command='python3 /opt/airflow/scripts/fetch_stocks.py'
    )

    
    task_nbp >> task_stocks