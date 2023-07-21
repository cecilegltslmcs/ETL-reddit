import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from ETL_Utils.etl_job import extract_data, transform_data, load_data, etl_job_complete
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "Cecile",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
  dag_id = 'etl_reddit_pipeline',
  schedule="@daily",
  description="A simple Reddit ETL pipeline using Python and Apache Airflow",
  default_args=default_args,
  catchup = False,
  max_active_runs = 1, 
  tags=["ETL", "Reddit"]
  ) as dag:

   start_pipeline = EmptyOperator(
		task_id='start_pipeline',
	)

   etl_task = PythonOperator(
    task_id = 'etl_task',
    python_callable = etl_job_complete
  )
   

   end_pipeline = EmptyOperator(
       task_id='end_pipeline',
  )
   
   start_pipeline >> etl_task >> end_pipeline
