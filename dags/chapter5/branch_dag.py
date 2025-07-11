import random

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="branch_dag",
    start_date=days_ago(3),
    schedule_interval=None,
    catchup=False
) as dag:
  fetch_sales_v1 = PythonOperator(
      task_id="fetch_sales_v1",
      python_callable=lambda: print("fetch_sales_v1 called"),
  )

  fetch_sales_v2 = PythonOperator(
      task_id="fetch_sales_v2",
      python_callable=lambda: print("fetch_sales_v2 called"),
  )

  fetch_weather = PythonOperator(
      task_id="fetch_weather",
      python_callable=lambda: print("fetch_weather called"),
  )

  clean_weather = PythonOperator(
      task_id="clean_weather",
      python_callable=lambda: print("clean_weather called"),
  )

  clean_sales_v1 = PythonOperator(
      task_id="clean_sales_v1",
      python_callable=lambda: print("clean_sales_v1 called"),
  )
  clean_sales_v2 = PythonOperator(
      task_id="clean_sales_v2",
      python_callable=lambda: print("clean_sales_v2 called"),
  )

  pick_version = BranchPythonOperator(
      task_id="pick_version",
      # Return task_id to execute
      python_callable=lambda: "fetch_sales_v1"
      if random.randrange(1, 3) % 2 == 0 else "fetch_sales_v2",

      provide_context=True,
  )

  picked_version = DummyOperator(
      task_id="selected_sales_version",
      trigger_rule='none_failed'
  )

  notify = PythonOperator(
      task_id="notify",
      trigger_rule="none_failed",
      python_callable=lambda: print("notify called"),
  )

  start_dag = DummyOperator(
      task_id="start_dag",
      dag=dag,
  )


  def _deploy(**context):
    if context['execution_date'].year < 2025:
      print("Skipping deployment for execution date before 2025.")
    else:
      print("Deploying the DAG...")


  deploy = PythonOperator(
      task_id='deploy',
      python_callable=_deploy,
  )

  start_dag >> [pick_version, fetch_weather]
  pick_version >> [fetch_sales_v1, fetch_sales_v2]
  fetch_sales_v1 >> clean_sales_v1
  fetch_sales_v2 >> clean_sales_v2
  fetch_weather >> clean_weather
  [clean_sales_v1, clean_sales_v2] >> picked_version
  [picked_version, clean_weather] >> notify >> deploy
