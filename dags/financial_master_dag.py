from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 2,                          # Jeśli skrypt padnie, spróbuj jeszcze 2 razy
    'retry_delay': timedelta(minutes=5),   # Odczekaj 5 min przed kolejną próbą
}

with DAG(
    'financial_system_pipeline',
    default_args=default_args,
    description='Automatyczny pobór NBP i Stocks z obsługa bledów',
    schedule_interval='0 18 * * *',        # Codziennie o 18:00 (po zamknieciu gieldy)
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    # 1. Job NBP
    task_nbp = BashOperator(
        task_id='nbp_data_ingest',
        bash_command='python3 /opt/airflow/scripts/fetch_nbp.py'
    )

    # 2. Job Stocks (ruszy TYLKO jeśli NBP sie uda)
    task_stocks = BashOperator(
        task_id='stocks_data_ingest',
        bash_command='python3 /opt/airflow/scripts/fetch_stocks.py'
    )

    # Ustawienie zaleznosci (Error Check: Task 2 czeka na Task 1)
    task_nbp >> task_stocks