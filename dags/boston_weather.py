import pendulum
import os
import glob
from os.path import join
import pandas as pd
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
from airflow.hooks.S3_hook import S3Hook


with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2023, 4, 2, tz="UTC"),
    schedule_interval='0 0 * * 1', # executar toda segunda feira
) as dag:

    task1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/tmp/raw/source=weather_api/week={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados(data_interval_end):
        city = 'Boston'
        key = 'SB9JCKN5YBUY3F5VYSQ322XYR'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)

        file_path = f'/tmp/raw/source=weather_api/week={data_interval_end}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime','tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    task2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    def local_to_s3(bucket_name, data_interval_end):
        s3 = S3Hook()

        for f in glob.glob(f'./tmp/raw/source=weather_api/week={data_interval_end}/*.csv'):
            key = 'weather/'+f.split('/')[-1]
            s3.load_file(filename=f, bucket_name=bucket_name,
                        replace=True, key=key)

    task3 = PythonOperator(
        task_id='weather_to_s3_stage',
        python_callable=local_to_s3,
        op_kwargs={
            'bucket_name': 'guidsantos-datalake-raw',
            'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'
        },
    )

    task1 >> task2 >> task3