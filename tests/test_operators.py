import csv

import requests
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from pytest_mock import MockerFixture

from airflow_movielens.hooks import MovielensHook
from airflow_movielens.operators import MovielensPopularityOperator
from chapter9.example_operators import JsonToCsvOperator


def test_example():
    task = BashOperator(
        task_id="test",
        bash_command="echo 'Hello, World!'",
        do_xcom_push=True,
    )
    result = task.execute(context={})
    assert result == "Hello, World!"



def test_movielenspopularityoperator(mocker: MockerFixture):
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
    mock_session = requests.Session()
    mock_session.auth = ("airflow", "airflow")
    mock_get = mocker.patch.object(
        MovielensHook,
        "get_conn",
        return_value= (
            mock_session,
            "http://localhost:5001"
        )
    )

    # when
    expected_count = 5
    task = MovielensPopularityOperator(
        task_id="task",
        conn_id="test",
        start_date="2019-01-10",
        end_date="2019-01-10",
        top_n=expected_count
    )
    result = task.execute(context={})


    # then
    assert mock_get.call_count == 2 # get_ratings, _get_with_pagination
    assert len(result) == expected_count


def test_json_to_csv_operator(tmp_path): # tmp_path는 고정으로 사용
    # given
    input_file_path = tmp_path / "input.json"
    output_file_path = tmp_path / "output.csv"

    input_data = [
        {"name": "Alice", "age": "30", "city": "New York"},
        {"name": "Bob", "age": "25", "city": "Los Angeles"},
        {"name": "Charlie", "age": "35", "city": "Chicago"},
    ]

    with open(input_file_path, 'w') as f:
        import json
        json.dump(input_data, f)

    # when
    operator = JsonToCsvOperator(
        task_id="json_to_csv",
        json_file_path=str(input_file_path),
        csv_file_path=str(output_file_path)
    )

    operator.execute(context={})
    with open(output_file_path, 'r') as f:
        reader = csv.DictReader(f)
        result = [dict(row) for row in reader]

    assert result == input_data