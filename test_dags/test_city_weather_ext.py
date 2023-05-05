import requests
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime as dt
from city_requests import get_city
from transform_20 import transform_data_into_csv_20
from transform_ALL import transform_all_data_into_csv
from linreg import linreg_score
from dtree import dtree_score
from randfor import randfor_score
from compare_scores import compare_and_save

city_weather_dag = DAG(
    dag_id='city_weather_dag',
    description='Querying weather in London, Paris, Washington',
    tags='datascientest',
    schedule_interval='* * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': dt.datetime(2023, 2, 20)
    }
)

task1 = PythonOperator(
    task_id='get_city_request',
    dag=city_weather_dag,
    python_callable=get_city
)

task2 = PythonOperator(
    task_id='transform_20_json_data.csv',
    dag=city_weather_dag,
    python_callable=transform_data_into_csv_20
)

task3 = PythonOperator(
    task_id='transform_ALL_json_fulldata.csv',
    dag=city_weather_dag,
    python_callable=transform_all_data_into_csv
)

task4_1 = PythonOperator(
    task_id='linear_regression_score',
    dag=city_weather_dag,
    python_callable=linreg_score
)

task4_2 = PythonOperator(
    task_id='decision_tree_regression_score',
    dag=city_weather_dag,
    python_callable=dtree_score
)

task4_3 = PythonOperator(
    task_id='random_forrest_regression_score',
    dag=city_weather_dag,
    python_callable=randfor_score
)

task5 = PythonOperator(
    task_id='comparing_models',
    dag=city_weather_dag,
    python_callable=compare_and_save
)

task1 >> [task2, task3]

task3 >> [task4_1, task4_2, task4_3]

[task4_1, task4_2, task4_3] >> task5

