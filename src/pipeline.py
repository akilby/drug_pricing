"""
Initiate ETL processes
Pipeline:
    Query from Reddit >> Process text data >> Persist in Mongo

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('tutorial', default_args=default_args,
          schedule_interval=timedelta(days=1))


def main():
    """Executes the loader."""

    # constants
    db = "opioids"
    """
    # connect to mongo client
    myclient = pymongo.MongoClient()
    db = myclient["opioid_test"]
    coll = db["comments"]
    comment = { "id": 1, "text": "hello world" }
    coll.insert_one(comment)
    """

    pass


if __name__ == "__main__":
    main()
