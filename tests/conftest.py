"""Conftest file."""
import datetime

import pytest
from airflow import DAG


@pytest.fixture
def test_dag(tmpdir):
    return DAG(
        "test_dag",
        default_args={"owner": "airflow",
                      "start_date": datetime.datetime(2019, 10, 10),
                      },
        template_searchpath=str(tmpdir),
        schedule_interval="@daily",
        catchup=False
    )