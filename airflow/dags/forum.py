"""
Defines a pipeline that extracts data from Reddit into Mongo.

Pipeline: Query from Reddit >> Persist in Mongo
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from utils.load import add_to_mongo, extract_subcomms

PRAW_ARGS = {
    'owner': 'charlie',
    'depends_on_past': True,
    'start_date': datetime.utcnow(),
    'email': ['cccdenhart@me.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

PRAW_DAG = DAG('praw', default_args=PRAW_ARGS,
               schedule_interval=timedelta(days=7))

TASK1 = PythonOperator(
    task_id="praw",
    python_callable=extract_subcomms,
    provide_context=True,
    dag=PRAW_DAG
)

TASK2 = PythonOperator(
    task_id="praw_mongo",
    python_callable=add_to_mongo,
    provide_context=True,
    dag=PRAW_DAG
)

TASK1 >> TASK2
