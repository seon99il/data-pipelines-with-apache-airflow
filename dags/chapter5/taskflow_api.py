import uuid

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

with DAG(
    dag_id="taskflow_api",
    start_date=days_ago(3),
    schedule_interval=None,
    catchup=False,
) as dag:
  @task
  def train_model(**context):
    model_id = str(uuid.uuid4())
    return model_id


  @task
  def deploy_model(model_id):
    print("model_id: ", model_id)


  model_id = train_model()
  deploy_model(model_id)
