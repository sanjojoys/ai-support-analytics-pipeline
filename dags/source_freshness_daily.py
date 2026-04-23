from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'source_freshness_daily', 
    schedule='0 1 * * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    
    check_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command='cd /usr/local/airflow/ai_support_analytics && dbt source freshness --profiles-dir /usr/local/airflow/.dbt'
    )                                                               