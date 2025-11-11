import os

import pytest
import requests
from airflow.models import Connection, DagBag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pytest_docker_tools import fetch, container
from pytest_mock import MockerFixture

from airflow_movielens.hooks import MovielensHook
from chapter9.operators import MovielensToPostgresOperator

postgres_image = fetch(repository="postgres:latest")
postgres = container(
    image="{postgres_image.id}",
    environment={"POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpass"},
    ports={"5432/tcp": None},
    volumes={
        os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
        }
    },
)

@pytest.fixture(scope="module")
def mock_session():
    mock_session = requests.Session()
    mock_session.auth = ("airflow", "airflow")

    return mock_session

def test_postgres_connection_url(postgres):
    try:
        print(postgres.url)
    except Exception:
        print("Container Logs: ", postgres.logs())


def test_movielens_to_postgres_operator(mocker: MockerFixture, test_dag, postgres, mock_session):
    # given
    mocker.patch.object(
        MovielensHook,
        "get_conn",
        return_value=(
            mock_session,
            "http://localhost:5001",
        ))

    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            conn_id="test_postgres",
            login="testuser",
            password="testpass",
            host="localhost",
            port=postgres.ports["5432/tcp"][0],
        )
    )

    dab_bag = DagBag("../conftest.py", include_examples=False)
    assert dab_bag.import_errors == {}

    # when
    task = MovielensToPostgresOperator(
        task_id="movielens_to_postgres",
        movielens_conn_id="test_movielens",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        postgres_conn_id="postgres_id",
        insert_query="INSERT INTO movielens (movieid, rating, ratingtimestamp, userid, scrapetime)"
                     "VALUES ({0}, '{{ macros.datetime.now() }}')",
        dag=test_dag,
    )

    pg_hook = PostgresHook()
    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count == 0

    task.run(
        start_date=test_dag.default_args["start_date"],
        end_date=test_dag.default_args["start_date"],
        ignore_ti_state=True,
    )

    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]

    assert row_count > 0
