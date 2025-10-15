from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime


with DAG(
    dag_id="dependency",
    start_date=datetime(2025, 10, 3),
    schedule="@daily"
):
    
    start = EmptyOperator(
        task_id="start"
    )

    fetch_customer = EmptyOperator(
        task_id="fetch_customer"
    )

    fetch_complaint = EmptyOperator(
        task_id="fetch_complaint"
    )
    
    clean_customer = EmptyOperator(
        task_id="clean_customer"
    )

    clean_complaint = EmptyOperator(
        task_id="clean_complaint"
    )

    join_datasets = EmptyOperator(
        task_id="join_datasets"
    )

    train_model = EmptyOperator(
        task_id="train_model"
    )

    deploy_model = EmptyOperator(
        task_id="deploy_model"
    )

    # Fan out
    start >> [fetch_customer, fetch_complaint]
    fetch_customer >> clean_customer
    fetch_complaint >> clean_complaint
    [clean_customer, clean_complaint] >> join_datasets
    join_datasets >> train_model >> deploy_model
