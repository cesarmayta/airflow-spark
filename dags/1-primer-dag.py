from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_hello():
    print("Hola desde un dag con PythonOperator!")
    
with DAG(
    dag_id='1-primer-dag',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    description='Mi Primer Dag'
) as dag:
    
    t1 = PythonOperator(
        task_id='t1',
        python_callable=print_hello,
    )