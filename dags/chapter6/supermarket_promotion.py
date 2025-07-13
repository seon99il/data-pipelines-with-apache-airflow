from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago


### SUPERMARKET PROMOTION DAG ###
# 일일 프로모션 데이터는 다음 날 16:00 ~ 02:00 사이에 공유 저장소로 들어오며,
# 비 정규적인 시간에 도착


def process_supermarket(supermarket_id: str):
  print("Processing data for supermarket:", supermarket_id)


def wait_for_supermarket(supermarket_id: str):
  supermarket_path = Path("/opt/airflow/data/" + supermarket_id)
  data_files = supermarket_path.glob("data-*.csv")
  success_file = supermarket_path / "_SUCCESS.txt"

  return data_files and success_file.exists()


with DAG(
    dag_id="supermarket_promotion",
    start_date=days_ago(3),
    schedule_interval=None,
    catchup=False,
    concurrency=50
) as dag:
  wait_for_supermarket_1 = PythonSensor(
      task_id="wait_for_supermarket_1",
      python_callable=wait_for_supermarket,
      mode="reschedule",
      op_kwargs={
        "supermarket_id": "supermarket_1",
      }
  )

  process_supermarket_1 = PythonOperator(
      task_id='process_supermarket_data_1',
      python_callable=process_supermarket,
      op_kwargs={
        "supermarket_id": "supermarket_1",
      }
  )

  wait_for_supermarket_2 = PythonSensor(
      task_id="wait_for_supermarket_2",
      python_callable=wait_for_supermarket,
      mode="reschedule",
      op_kwargs={
        "supermarket_id": "supermarket_2",
      }
  )

  process_supermarket_2 = PythonOperator(
      task_id='process_supermarket_data_2',
      python_callable=process_supermarket,
      op_kwargs={
        "supermarket_id": "supermarket_2",
      }
  )

  start_dag = DummyOperator(
      task_id="start_dag",
      dag=dag,
  )

  create_metrics = PythonOperator(
      task_id="create_metrics",
      python_callable=lambda: print("Creating metrics..."),
      dag=dag,
  )

start_dag >> [wait_for_supermarket_1, wait_for_supermarket_2]
wait_for_supermarket_1 >> process_supermarket_1
wait_for_supermarket_2 >> process_supermarket_2
[process_supermarket_1, process_supermarket_2] >> create_metrics
