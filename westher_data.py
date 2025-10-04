from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator

dag = DAG(
    dag_id="weather_data",
    start_date=datetime(2025, 10, 2),
    schedule=None,
)


def _fetch_weather():
    print("Feteched weather data")


def _transform_weather():
    print("Weather data transformed")


def _load_data():
    print("Loaded data successfuly")


fetch_weather = PythonOperator(
    task_id="fetch_weather",
    python_callable=_fetch_weather,
    dag=dag
)

transform_weather = PythonOperator(
    task_id="transform_weather",
    python_callable=_transform_weather,
    dag=dag
)

load_data = PythonOperator(
    task_id="load_data",
    python_callable=_load_data,
    dag=dag
)

fetch_weather >> transform_weather >> load_data
