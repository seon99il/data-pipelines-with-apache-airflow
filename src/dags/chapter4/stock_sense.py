import os
from urllib import request, error

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="stock_sense",
    start_date=days_ago(3),
    schedule_interval="@hourly",
    catchup=False,  # 이전 날짜에 대한 실행을 건너뜀
) as dag:

    def getData(year: int, month, day, hour, output_path, **context):
        url = (
            "https://dumps.wikimedia.org/other/pageviews/"
            f"{year}/{year}-{month:0>2}/"
            f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
        print("url:", url)

        try:
            request.urlretrieve(url, output_path)
        except error.HTTPError as e:
            print(f"Failed to retrieve data from {url}: {e}")
            if e.code == 404:
                print("Data not found for the specified date and hour.")
            else:
                raise

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=getData,
        op_kwargs={
            "year": "{{ execution_date.year }}",
            "month": "{{ execution_date.month }}",
            "day": "{{ execution_date.day }}",
            "hour": "{{ execution_date.hour }}",
            "_output_path": "/tmp/wikipageviews.gz",
        },
    )

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="[ ! -f /tmp/wikipageviews.gz ] || gzip -d -f /tmp/wikipageviews.gz",
        output_encoding="utf-8",
    )

    def _fetch_pageviews(page_names, execution_date):
        page_names = set(map(lambda x: str(x).upper(), page_names))
        result = dict.fromkeys(page_names, 0)

        wikipageviews_path = "/tmp/wikipageviews"
        if os.path.exists(wikipageviews_path):
            with open(wikipageviews_path, "r") as file:
                for line in file:
                    print("line:", line)
                    domain_code, page_title, view_counts, *_ = line.split(" ")
                    if domain_code == "en" and str(page_title).upper() in page_names:
                        result[page_title] = view_counts
            print("result:", result)
        else:
            print(f"File not found: {wikipageviews_path}")

        with open(f"/tmp/postgres_query.sql", "w") as file:
            for pagename, pageviewcount in result.items():
                file.write(
                    "INSERT INTO PAGEVIEW_COUNTS VALUES ("
                    "'{}', {}, '{}');\n".format(pagename, pageviewcount, execution_date)
                )

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "page_names": list(
                map(
                    lambda x: str(x).upper(),
                    ["Google", "Amazon", "Apple", "Microsoft", "Facebook"],
                )
            ),
        },
        dag=dag,
    )

    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        postgres_conn_id="postgres_default",
        sql="postgres_query.sql",
        dag=dag,
    )

    get_data >> extract_gz >> fetch_pageviews >> write_to_postgres
