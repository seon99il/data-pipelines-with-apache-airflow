from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="xcoms",
    start_date=days_ago(3),
    schedule_interval=None,
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=lambda: "model_id_1",
    )

    task2 = PythonOperator(
        task_id="task2",
        python_callable=lambda: print("model_id_2"),
    )

    def _deploy_model(**context):
        model_ids = context["ti"].xcom_pull(
            task_ids=["task2", "task1"],
            key=None,  # Default to pulling the return value
        )
        print("model_id: ", str(model_ids))

    deploy_model = PythonOperator(
        task_id="deploy_model",
        python_callable=_deploy_model,
    )

    [task1, task2] >> deploy_model
