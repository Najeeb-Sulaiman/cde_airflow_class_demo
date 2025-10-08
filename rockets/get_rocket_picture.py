from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from rockets.include.get_launches import _get_pictures

output_path = "/opt/airflow/dags/rockets/launches/launches.json"
api_url = "https://ll.thespacedevs.com/2.0.0/launch/upcoming/"

with DAG(
    dag_id="rocket_pictures",
    start_date=datetime(2025, 10, 3),
    schedule="@daily"
):

    download_launches = BashOperator(
        task_id="download_launches",
        bash_command=f"curl -o {output_path} -L {api_url}"
    )

    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures
    )

    # notify = BashOperator(
    #     task_id="notify",
    #     bash_command='echo "There are now $(ls /opt/airflow/dags/rockets/images/ | wc -l) images"'
    # )

    send_notification = EmailOperator(
        task_id="send_notification",
        to=["sulaimannajeebadesoji@gmail.com", "bojzino128@gmail.com", "korexma011@gmail.com"],
        subject="Rocket Image Download Complete for {{ ds }}",
        html_content="""
        <h3>Rocket Launches Update</h3>
        <p>Hi John, The Airflow pipeline ran successfully</p>
        <p>Total Tocket images downloaded: <b>5</b> </p>
        <p>Find attached the images</p>
        <p>Kind Regards, CDE Airflow team</p>
        """,
        conn_id="smtp_conn"
        )

    download_launches >> get_pictures >> send_notification
