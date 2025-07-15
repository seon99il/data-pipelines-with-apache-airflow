import os
from datetime import datetime, timedelta
from urllib import request, error

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # Retry delay in seconds
}


def download_data(execution_date_str, hour, **context):
    dt = datetime.strptime(execution_date_str, "%Y-%m-%d")

    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{dt.year}/{dt.year}-{dt.month:02}/"
        f"pageviews-{dt.year}{dt.month:02}{dt.day:02}-{hour:02}0000.gz"
    )

    output_path = f"/tmp//wikipageviews_{hour}.gz"

    try:
        request.urlretrieve(url, output_path)

    except error.HTTPError as e:
        print(f"Failed to retrieve data from {url}: {e}")
        if e.code == 404:
            print("Data not found for the specified date and hour.")
        else:
            raise


def extract(hour):
    os.system(f"gzip -df /tmp/wikipageviews_{hour}.gz")


def fetch_views(hour, page_names, **kwargs):
    path = f"/tmp/wikipageviews_{hour}"
    page_names = set(name.upper() for name in page_names)
    result = dict.fromkeys(page_names, 0)

    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                parts = line.split(" ")
                if len(parts) < 3:
                    continue
                domain_code, page_title, view_count = parts[:3]
                if domain_code == "en" and page_title.upper() in page_names:
                    result[page_title.upper()] += int(view_count)

    kwargs["ti"].xcom_push(key=f"pageviews_{hour}", value=result)


def aggregate_results(**kwargs):
    total = {}
    for hour in range(24):
        hour_data = kwargs["ti"].xcom_pull(
            task_ids=f"fetch_hourly_data.fetch_{hour}", key=f"pageviews_{hour}"
        )
        if not hour_data:
            continue
        for k, v in hour_data.items():
            total[k] = total.get(k, 0) + int(v)

    execution_date = kwargs["ds"]
    sql_lines = [
        f"INSERT INTO pageview_counts VALUES ('{k}', {v}, '{execution_date}');"
        for k, v in total.items()
    ]
    kwargs["ti"].xcom_push(key="sql_query", value="\n".join(sql_lines))


with DAG(
    dag_id="daily_pageviews_summary",
    start_date=days_ago(3),
    schedule_interval="@daily",
    catchup=True,
    default_args=default_args,
) as dag:
    with TaskGroup("fetch_hourly_data") as fetch_hourly_data:
        for hour in range(24):
            download = PythonOperator(
                task_id=f"download_{hour}",
                python_callable=download_data,
                op_kwargs={
                    "hour": hour,
                    "execution_date_str": "{{ ds }}",
                },
            )

            extract_task = PythonOperator(
                task_id=f"extract_{hour}",
                python_callable=extract,
                op_kwargs={"hour": hour},
            )

            fetch = PythonOperator(
                task_id=f"fetch_{hour}",
                python_callable=fetch_views,
                op_kwargs={
                    "hour": hour,
                    "page_names": [
                        "Google",
                        "Amazon",
                        "Apple",
                        "Microsoft",
                        "Facebook",
                    ],
                },
            )

            download >> extract_task >> fetch

    aggregate = PythonOperator(
        task_id="aggregate_sql", python_callable=aggregate_results, provide_context=True
    )

    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        postgres_conn_id="postgres_default",
        sql="{{ ti.xcom_pull(task_ids='aggregate_sql', key='sql_query') }}",
    )

    fetch_hourly_data >> aggregate >> write_to_postgres
