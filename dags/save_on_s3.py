from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator


with DAG(
    "save_s3",
    start_date=days_ago(1),
    schedule_interval="@daily",
) as dag:
    Upload_to_s3 = S3CreateObjectOperator(
        task_id="Upload-to-S3",
        aws_conn_id= 'AWS_CONN',
        s3_bucket='guidsantos-datalake-raw',
        s3_key='rajesh/Api_Data.txt',
        data="texto",    
    )

    Upload_to_s3
 