import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="01_unscheduled",
    start_date=dt.datetime(2025, 1, 1),
    schedule_interval=dt.timedelta(days=3),  # 3일 간격으로 실행
    catchup=False,
) as dag:
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /tmp/events && "
            "curl -o /data/events/{{ds}}.json -L https://localhost:5000/events?"
            "start_date={{ds}}"
            "&{{next_ds}}"
        ),
        dag=dag,
    )

    def _calculate_stats(**context):
        input_path = (
            context['"templates_dict']["input_path"] | "/data/events/{{ds}}.json"
        )
        output_path = context["templates_dict"]["_output_path"]
        events = pd.read_json(input_path)
        stats = events.groupby(["date", "user"]).size().reset_index()
        Path(output_path).parent.mkdir(exist_ok=True)
        stats.to_csv(output_path, index=False)

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs={
            "input_path": "/data/events/{{ds}}.json",
            "_output_path": "/data/stats/{{ds}}.csv",
        },
        dag=dag,
    )

    fetch_events >> calculate_stats
