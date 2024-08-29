from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

default_args = {
		'owner': 'DEMO ETL WITH AIRFLOW AND SPARK',
		'start_date': datetime(2024, 8, 1),
		'retries': 0,
		'retry_delay': timedelta(minutes=1)
}

with DAG('demo-dag', default_args=default_args, catchup=False) as dag:

	etl = SparkSubmitOperator(
        application = '/opt/airflow/data/demo-etl.py',
        conn_id = 'spark',
        task_id = 'etl-spark',
		jars = '/opt/spark/data/postgresql-42.2.5.jar'
        )

	etl