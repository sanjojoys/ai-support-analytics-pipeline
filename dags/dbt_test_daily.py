from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'dbt_test_daily', 
    schedule='0 3 * * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /usr/local/airflow/ai_support_analytics && dbt test --profiles-dir /usr/local/airflow/.dbt'
    )

    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /usr/local/airflow/ai_support_analytics && dbt docs generate --profiles-dir /usr/local/airflow/.dbt'
    )

    dbt_test >> dbt_docs