from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

start_date = datetime.now() - timedelta(days=3)

with DAG(
    dag_id="ingest_supermarket_data",
    start_date=start_date,
    schedule_interval=None,
    catchup=False,
) as dag:
    trigger_create_metrics_dags = []
    for supermarket_id in range(1, 5):
        task_id = f"trigger_create_matrics_dag_supermarket_{supermarket_id}"
        trigger_create_metrics_dag = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="supermarket_promotion",
            conf={
                "supermarket_id": supermarket_id,
            },
            dag=dag,
        )
        trigger_create_metrics_dags.append(trigger_create_metrics_dag)

    notify = PythonOperator(
        task_id="notify",
        python_callable=lambda: print(  # only can check triggerred dags run
            "All supermarket data ingestion tasks triggered."
        ),
    )

trigger_create_metrics_dags >> notify
