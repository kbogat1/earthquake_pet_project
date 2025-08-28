import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


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
    'start_date': pendulum.datetime(2025, 8, 22, tz="Europe/Moscow"),
    'catchup': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def get_data(**context):
    con = duckdb.connect()

    con.sql(
    f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

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

