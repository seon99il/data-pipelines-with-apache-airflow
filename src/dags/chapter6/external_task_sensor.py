from datetime import timedelta, datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

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
    BashOperator(task_id="copy_to_raw", dag=dag1, bash_command="echo 'Copying data to raw layer...'")
)

wait = ExternalTaskSensor(
    task_id="wait_for_process_supermarket",
    external_dag_id="external_task_sensor_target",
    external_task_id="process_supermarket",
    dag=dag2,
)

report = BashOperator(task_id="report", dag=dag2, bash_command="echo 'Data processing completed. Generating report...'")

wait >> report

## Cpoy to raw >> process supermarket > (externalTaskSensor) > wait for process supermarket >> report
