import csv
import io
import os
from typing import Any

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.utils.dates import days_ago


class PostgresToCSVOperator(BaseOperator):

    def __init__(self, postgres_conn_id, query, csv_file_path, **kwargs):
        super().__init__(**kwargs)

        self._postgres_conn_id = postgres_conn_id
        self.query = query
        self.csv_file_path = csv_file_path

    def execute(self, context: Context) -> Any:
        ## Connect to Postgres and execute the query
        postgres_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)

        results = postgres_hook.get_records(self.query)

        data_buffer = io.StringIO()
        csv_writer = csv.writer(data_buffer, lineterminator=os.linesep)
        csv_writer.writerow(results)
        data_buffer_binary = data_buffer.getvalue().encode("utf-8")

        with open(self.csv_file_path, "wb") as csv_file:
            csv_file.write(data_buffer_binary)


with DAG(
    dag_id="postgres_to_csv",
    start_date=days_ago(3),
    schedule_interval=None,
    catchup=False,
) as dag:
    postgres_to_csv = PostgresToCSVOperator(
        task_id="postgres_to_csv",
        postgres_conn_id="postgres_default",
        csv_file_path="/tmp/pageview_counts.csv",
        query="SELECT * FROM public.pageview_counts",
        dag=dag,
    )

    check_result = BashOperator(
        task_id="check_result", bash_command="cat /tmp/pageview_counts.csv", dag=dag
    )

    postgres_to_csv >> check_result
