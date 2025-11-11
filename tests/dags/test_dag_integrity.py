import glob
import importlib.util
import os

import pytest
from airflow import DAG

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "src/dags/**/*.py")

DAG_FILES = glob.glob(DAG_PATH, recursive=True)


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_syntax(dag_file):
    module_name, _ = os.path.split(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)

    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)

    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
    print(dag_objects)

    assert dag_objects

    for dag in dag_objects:
        dag.dag_id.startswith(("import", "export"))