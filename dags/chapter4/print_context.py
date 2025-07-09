import datetime

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="print_context",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
    catchup=False,
) as dag:
  def _print_context(execution_date, **context):
    print("----- Airflow Context -----")
    print("Context:", context)
    context['execution_date'] = execution_date - datetime.timedelta(
        days=1)

    print("Execution Date:", context['execution_date'])
    print("----- End of Context -----")
    # ds: Execution date in YYYY-MM-DD format
    # ds_nodash: Execution date in YYYYMMDD format
    # execution_date: Execution date as a datetime object
    # execution_date_success: Execution date as a string
    # (prev, next)
    # test_mode: True if the DAG is running in test mode


  print_context = PythonOperator(
      task_id="print_context",
      python_callable=_print_context,
      provide_context=True,  # Ensure context is passed to the callable
      dag=dag,
  )


  def _check_execution_date(**context):
    print("Check Execution Date:", context["execution_date"])


  check_execution_date = PythonOperator(
      task_id="check_execution_date",
      python_callable=_check_execution_date,
      dag=dag,
  )

  print_context >> check_execution_date
