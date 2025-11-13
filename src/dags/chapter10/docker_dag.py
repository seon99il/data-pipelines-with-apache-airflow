import datetime as dt

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="01_docker",
    description="Fetches ratings from the Movielens API using Docker.",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 3),
    schedule_interval="@daily",
) as dag:

    fetch_ratings = DockerOperator(
        task_id="fetch_ratings",
        image="manning-airflow/movielens-fetch",
        auto_remove="force",
        mount_tmp_dir=False,
        command=[
            "fetch-ratings",
            "--start_date",
            "{{ds}}",
            "--end_date",
            "{{next_ds}}",
            "--output_path",
            "/data/ratings/{{ds}}.json",
            "--user",
            "airflow",
            "--password",
            "airflow",
            "--host",
            "http://host.docker.internal:5001",
        ],
        network_mode="airflow",
        mounts=[
            Mount(
                source="/tmp/airflow/data",  # 호스트 경로
                target="/data",  # 컨테이너 경로
                type="bind",  # 바인딩 마운트 타입 지정
            )
        ],
    )

    rank_movies = DockerOperator(
        task_id="rank_movies",
        image="manning-airflow/movielens-rank",
        auto_remove="force",
        command=[
            "rank-movies",
            "--input_path",
            "/data/ratings/{{ds}}.json",
            "--output_path",
            "/data/rankings/{{ds}}.csv",
        ],
        mounts=[
            Mount(
                source="/tmp/airflow/data",  # 호스트 경로
                target="/data",  # 컨테이너 경로
                type="bind",  # 바인딩 마운트 타입 지정
            )
        ],
    )

    fetch_ratings >> rank_movies