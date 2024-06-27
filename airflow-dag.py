from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate DAG object
dag = DAG(
    'pyspark_keywords_dag',
    default_args=default_args,
    description='DAG to run the PySpark script for keywords extraction',
    schedule_interval='@weekly',  # Run weekly
)

# Define the task to execute the PySpark script using BashOperator
run_pyspark_script = BashOperator(
    task_id='run_pyspark_script',
    bash_command='python /path/to/pyspark_keywords.py',  # Replace with the actual path
    dag=dag,
)

# Add the task to the DAG
run_pyspark_script