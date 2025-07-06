import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id='download_rocket_launches',
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

download_launches = BashOperator(
    task_id='download_launches',
    bash_command='curl -o /tmp/rocket_launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming',
    dag=dag,
)

get_pictures = BashOperator(
    task_id="get_pictures",
    bash_command='python3 /opt/airflow/dags/chapter2/scripts/get_pictures.py',
    dag=dag
)

notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls /tmp/images | wc -l) images."',
    dag=dag
)

download_launches >> get_pictures >> notify
