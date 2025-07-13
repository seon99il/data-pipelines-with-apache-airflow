from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

### SUPERMARKET PROMOTION DAG ###
# 일일 프로모션 데이터는 다음 날 16:00 ~ 02:00 사이에 공유 저장소로 들어오며,
# 비 정규적인 시간에 도착


start_date = datetime.now() - timedelta(days=3)


def process_supermarket(**context):
  print("Processing data for supermarket:",
        str(context['dag_run'].conf.get('supermarket_id')))


def wait_for_supermarket(**context):
  supermarket_path = Path(
      "/opt/airflow/data/" + str(context['dag_run'].conf.get('supermarket_id')))
  data_files = supermarket_path.glob("data-*.csv")
  success_file = supermarket_path / "_SUCCESS.txt"

  return data_files and success_file.exists()


with DAG(
    dag_id="supermarket_promotion",
    start_date=start_date,
    schedule_interval=None,
    catchup=False,
    concurrency=50
) as dag:
  wait_for_supermarket = PythonSensor(
      task_id="wait_for_supermarket",
      python_callable=wait_for_supermarket,
      mode="reschedule",
  )

  process_supermarket = PythonOperator(
      task_id="process_supermarket",
      python_callable=process_supermarket,
  )

  create_metrics = PythonOperator(
      task_id="create_metrics",
      python_callable=lambda: print("Creating metrics..."),
      dag=dag,
  )

  wait_for_supermarket >> process_supermarket >> create_metrics
