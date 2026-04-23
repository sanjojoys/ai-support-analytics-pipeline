from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    'ingest_raw_daily',
    default_args={'retries': 1},
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    
    generate_data = BashOperator(
        task_id='generate_raw_files',
        # Added mkdir -p to ensure the data directory exists before running the generator
        # Changed --days 1 to --days 30 to fix the random range error
        bash_command='mkdir -p /usr/local/airflow/data && python /usr/local/airflow/generators/generate_ai_support_data.py --outdir /usr/local/airflow/data --accounts 500 --days 100 --seed 42'
    )
    load_raw = BashOperator(
        task_id='load_to_snowflake',
        bash_command='echo "Executing SnowSQL or Python load script here..."'
    )

    generate_data >> load_raw