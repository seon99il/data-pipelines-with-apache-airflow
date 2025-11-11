import datetime as dt
import logging
import json
import os

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_movielens.operators import MovielensFetchRatingsOperator
from airflow_movielens.ranking import rank_movies_by_rating
from airflow_movielens.sensors import MovielensRatingsSensor
from common.print_common import print_greeting


# pylint: disable=logging-format-interpolation


def test_package():
    print_greeting()



def _fetch_ratings(conn_id, templates_dict, batch_size=1000, **_):
    logging.getLogger(__name__)

    start_date = templates_dict["start_date"]
    end_date = templates_dict["_end_date"]
    output_path = templates_dict["_output_path"]

    logging.info(f"Fetching ratings from {start_date} to {end_date}")

    from airflow_movielens.hooks import MovielensHook
    hook = MovielensHook(conn_id)
    ratings = hook.get_ratings(start_date=start_date, end_date=end_date)


    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as file_:
        json.dump(ratings, fp=file_)


with DAG(
    dag_id="03_use_pacakge",
    description="Fetches ratings from the Movielens API using the Python Operator.",
    start_date=dt.datetime(2019, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    wait_for_ratings = MovielensRatingsSensor(
        task_id="wait_for_ratings",
        conn_id="movielens_api",
        start_date="2019-01-10",
        end_date="2019-01-11",
    )

    fetch_ratings = MovielensFetchRatingsOperator(
        task_id="fetch_ratings",
        conn_id="movielens_api",
        start_date="2019-01-10",
        end_date="2019-01-11",
        output_path="/opt/airflow/data/python/ratings/{{ds}}.json",
    )

    def _rank_movies(templates_dict,  min_ratings=2, **_):
        input_path = templates_dict["input_path"]
        output_path = templates_dict["output_path"]

        ratings = pd.read_json(input_path)
        ranking = rank_movies_by_rating(ratings, min_ratings=min_ratings)

        # Make sure output directory exists.
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        ranking.to_csv(output_path, index=True)

    rank_movies = PythonOperator(
        task_id="rank_movies",
        python_callable=_rank_movies,
        templates_dict={
            "input_path": "/opt/airflow/data/python/ratings/{{ ds }}.json",
            "output_path": "/opt/airflow/data/python/rankings/{{ ds }}.csv",
        },
    )



    wait_for_ratings >> fetch_ratings >> rank_movies


