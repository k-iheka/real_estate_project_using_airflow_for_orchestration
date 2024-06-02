# import necessary libraries
from datetime import timedelta
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from real_estate_etl import run_real_estate_etl
import psycopg2

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    'statr_date': datetime(2024, 4, 30),
    'email': ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG(
    'real_estate_dag',
    default_args = default_args,
    description = 'DAG script for realyo realestate Agency'
)

run_etl = PythonOperator(
    task_id = 'etl_process',
    python_callable = run_real_estate_etl,
    dag = dag
)

run_etl