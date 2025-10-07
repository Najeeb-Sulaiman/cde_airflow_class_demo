from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from include.get_launches import _get_pictures


with DAG(
    dag_id="rocket_pictures",
    start_date=datetime(2025, 10, 3),
    schedule="@daily"
):

    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /opt/airflow/dags/rockets/launches/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming/"
    )

    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures
    )

    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /opt/airflow/dags/rockets/images/ | wc -l) images"'
    )

    download_launches >> get_pictures >> notify
