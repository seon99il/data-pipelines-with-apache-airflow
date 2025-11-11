import datetime

import pytest
import requests
from airflow.models import Connection
from pytest_mock import MockerFixture

from airflow_movielens.hooks import MovielensHook
from airflow_movielens.operators import MovielensDownloadOperator


@pytest.fixture(scope="module")
def mock_session():
    mock_session = requests.Session()
    mock_session.auth = ("airflow", "airflow")

    return mock_session



def test_movielens_download_operator(mocker: MockerFixture, test_dag, mock_session, tmp_path):
    # given
    mocker.patch.object(
        MovielensHook,
        "get_connection",
        return_value=Connection(
            conn_id="test",
            login="airflow",
            password="airflow"
        )
    )
    mocker.patch.object(
        MovielensHook,
        "get_conn",
        return_value= (
            mock_session,
            "http://localhost:5001"
        )
    )

    task = MovielensDownloadOperator(
        task_id="test",
        conn_id="testconn",
        output_path=str(tmp_path / "{{ ds }}.json"),
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        dag=test_dag,
    )


    task.run(
        start_date=test_dag.default_args["start_date"],
        end_date=datetime.datetime(2019, 10, 11),
        ignore_ti_state=True,
    )

    # assert mock_execute.call_count == 1
