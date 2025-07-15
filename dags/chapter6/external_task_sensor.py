from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

dag1 = DAG(
    dag_id="external_task_sensor_target",
    schedule_interval="0 16 * * *",
    start_date=datetime.now() - timedelta(days=3),
    catchup=False,
)

dag2 = DAG(
    dag_id="external_task_sensor",
    schedule_interval="0 20 * * *",
    start_date=datetime.now() - timedelta(days=3),
    catchup=False,
)

(
    DummyOperator(task_id="copy_to_raw", dag=dag1)
    >> DummyOperator(task_id="process_supermarket", dag=dag1)
)

wait = ExternalTaskSensor(
    task_id="wait_for_process_supermarket",
    external_dag_id="external_task_sensor_target",
    external_task_id="process_supermarket",
    dag=dag2,
)

report = DummyOperator(task_id="report", dag=dag2)

wait >> report

## Cpoy to raw >> process supermarket > (externalTaskSensor) > wait for process supermarket >> report
