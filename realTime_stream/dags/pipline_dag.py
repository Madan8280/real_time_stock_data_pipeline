from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="stock_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_intreval= "@hourly",
    catchup=False
) as dag:

    raw_to_silver = BashOperator( 
        task_id="raw_to_silver",
        bash_command="python /opt/airflow/dags/script/raw_to_silver.py"
    )
