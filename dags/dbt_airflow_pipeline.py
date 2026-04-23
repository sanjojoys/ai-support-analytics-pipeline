from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_policy': 'on_failure',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# The profile directory inside the Docker container
DBT_PROJECT_DIR = '/usr/local/airflow/ai_support_analytics'
DBT_PROFILES_DIR = '/usr/local/airflow/.dbt'

with DAG(
    'ai_support_full_pipeline',
    default_args=default_args,
    description='Full end-to-end AI Support Analytics Pipeline',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    
    # 1. Generate Raw Files
    generate_raw = BashOperator(
        task_id='generate_raw_data',
        bash_command='mkdir -p /usr/local/airflow/data && python /usr/local/airflow/generators/generate_ai_support_data.py --outdir /usr/local/airflow/data --accounts 500 --days 30 --seed 42'
    )

    # 2. Load Raw Tables
    load_raw = BashOperator(
        task_id='load_to_snowflake',
        bash_command='echo "Loading files to Snowflake..."' 
    )

    # 3. Source Freshness
    check_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 4. dbt Seed
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILES_DIR}'
    )

    
    # 5. dbt Run (Double check this is the ONLY bash_command line for this task)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /usr/local/airflow/ai_support_analytics && dbt run --profiles-dir /usr/local/airflow/.dbt'
    )
    # 6. dbt Test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 7. dbt Docs Generate
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}'
    )

    # Setting the Flow (The "Chain")
    generate_raw >> load_raw >> check_freshness >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs