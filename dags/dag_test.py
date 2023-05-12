from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

with DAG(
    'test_dag',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:
    task_1 = EmptyOperator(task_id="task_1")
    task_1