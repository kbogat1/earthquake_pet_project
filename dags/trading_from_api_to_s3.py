import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from dags.raw_from_api_to_s3 import start

# Конфигурация DAG
OWNER = 'k.bogatyrev'
DAG_ID = 'trading_from_api_to_s3'

# Используемые таблицы в DAG
LAYER = 'raw'
SOURCE = 'alphavantage'

# S3
ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
SECRET_KEY = Variable.get("S3_SECRET_KEY")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

default_args = {
    'owner': OWNER,
    'start_date': pendulum(2025, 8, 22).format('YYYY-MM-DD'),
    'catchup': False,
    'retries': 1,
    'retry_delay': pendulum(minutes=5),
}

def get_data(**context):
    con = duckdb.connect()

    con.sql(
    f"""
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID {ACCESS_KEY},
    SECRET {SECRET_KEY},
    REGION 'us-west-rack-2'
);

COPY (
SELECT * FROM read_csv_auto('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo&datatype=csv')
) TO 's3://prod/{LAYER}/{SOURCE}/daily.parquet';
"""
    )
    con.close()

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 8 * * *",
    default_args=default_args,
    tags=["s3", "raw", "trading"],
    description=SHORT_DESCRIPTION,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start"
    )

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data
    )

    end = EmptyOperator(
        task_id="end"
    )

start >> get_data >> end

