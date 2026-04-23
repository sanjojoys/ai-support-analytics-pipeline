from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'dbt_run_daily', 
    schedule='0 2 * * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /usr/local/airflow/ai_support_analytics && dbt seed --profiles-dir /usr/local/airflow/.dbt'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /usr/local/airflow/ai_support_analytics && dbt run --profiles-dir /usr/local/airflow/.dbt'
    )

    dbt_seed >> dbt_run