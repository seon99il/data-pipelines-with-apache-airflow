import os

import pytest

DAG_PATH = os.path.join(os.path.dirname(__file__), '..', 'src/dags')

@pytest.fixture
def dag_bag():
    from airflow.models import DagBag
    return DagBag(dag_folder=DAG_PATH, include_examples=False)


def test_dag_loads(dag_bag):
    assert len(dag_bag.dags) > 0, "No DAGs found in the DAG bag"