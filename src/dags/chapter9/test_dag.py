from datetime import datetime

from airflow import DAG

from airflow_movielens.operators import MovielensDownloadOperator

with DAG(
    dag_id="test_dag",
    start_date=datetime(2019, 1, 10),
    default_args={
        "start_date": datetime(2019, 1, 11)
    },
    schedule_interval="@daily",
) as dag:

    task = MovielensDownloadOperator(
        task_id="test",
        conn_id="testconn",
        output_path="/opt/airflow/data/{{ ds }}.json",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
    )
