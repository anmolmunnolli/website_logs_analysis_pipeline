from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Define the default_args dictionary to pass common arguments to each task
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'transformations_dag',
    default_args=default_args,
    description='A DAG to run dbt models',
    schedule_interval=None,  # Set to None or a CRON expression
)

# Define the function to run the PySpark script
# def run_spark_transformations():
#     os.system('python /mnt/c/Users/anmol/Desktop/de/pyspark_scripts/spark_transformations.py')

# # Task to run the PySpark script
# run_spark_transformations_task = PythonOperator(
#     task_id='run_spark_transformations',
#     python_callable=run_spark_transformations,
#     dag=dag,
# )

# Task to run the dbt models
run_dbt_models_task = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /mnt/c/Users/anmol/Desktop/de/dbt/ecommerce_analysis && dbt run --models session_analysis',
    dag=dag,
)

# Set task dependencies
run_dbt_models_task
