from airflow.sdk import DAG, task
from pendulum import datetime


with DAG(
    dag_id="print_context",
    start_date=datetime(2025, 10, 2),
    schedule=None,
):

    @task
    def print_context(**kwargs):
        print(kwargs)
        return kwargs

    # @task
    # def print_context_1(**kwargs):
    #     print(kwargs)

    print_context
