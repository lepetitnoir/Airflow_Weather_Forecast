import pandas as pd
from airflow import DAG
from airflow.models.xcom import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago



from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor

from transform_data import transform_data_into_csv

from city_requests import get_city


def linreg_score(task_instance):
    X, y = prepare_data()
    lr_score = compute_model_score(LinearRegression(), X, y)
    task_instance.xcom_push(
        key="lr_model_score",
        value={'lr_model_score': lr_score}
    )

def dtree_score(task_instance):
    X, y = prepare_data()
    dt_score = compute_model_score(DecisionTreeRegressor(), X, y)
    task_instance.xcom_push(
        key="dt_model_score",
        value={'dt_model_score': dt_score}
    )

def randfor_score(task_instance):
    X, y = prepare_data()
    rf_score = compute_model_score(RandomForestRegressor(), X, y)
    task_instance.xcom_push(
        key="rf_model_score",
        value={'rf_model_score': rf_score}
    )


def compare_and_save(task_instance):
    X, y = prepare_data()
    lr_list = task_instance.xcom_pull(
            key="lr_model_score",
            task_ids=['linear_regression_score']
        )
    lr_score = lr_list[0].get('lr_model_score')
    
    dt_list = task_instance.xcom_pull(
            key="dt_model_score",
            task_ids=['decision_tree_regression_score']
        )
    dt_score = dt_list[0].get('dt_model_score')
    
    rf_list = task_instance.xcom_pull(
            key="rf_model_score",
            task_ids=['random_forrest_regression_score']
        )
    rf_score = rf_list[0].get('rf_model_score')

    if lr_score > dt_score and lr_score > rf_score:
        train_and_save_model(LinearRegression(), X, y, '/app/clean_data/best_model.pickle')
    elif dt_score > lr_score and dt_score > rf_score:
        train_and_save_model(DecisionTreeRegressor(), X, y, '/app/clean_data/best_model.pickle')
    else:
        train_and_save_model(RandomForestRegressor(), X, y, '/app/clean_data/best_model.pickle')
    return

city_weather_dag = DAG( 
    dag_id='city_weather_dag',
    description='Querying weather in London, Paris, Washington',
    tags=['datascientest', 'tutorial'],
    schedule_interval='* * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1)
    }
)

task1 = PythonOperator(
    task_id='get_city_request',
    dag=city_weather_dag,
    python_callable=get_city
)

task2 = PythonOperator(
    task_id='transform_data.csv',
    dag=city_weather_dag,
    python_callable=transform_data_into_csv
)

task3_1 = PythonOperator(
    task_id='linear_regression_score',
    dag=city_weather_dag,
    python_callable=linreg_score
)

task3_2 = PythonOperator(
    task_id='decision_tree_regression_score',
    dag=city_weather_dag,
    python_callable=dtree_score
)

task3_3 = PythonOperator(
    task_id='random_forrest_regression_score',
    dag=city_weather_dag,
    python_callable=randfor_score
)

task4 = PythonOperator(
    task_id='comparing_models',
    dag=city_weather_dag,
    python_callable=compare_and_save
)

task1 >> [task2]

task2 >> [task3_1, task3_2, task3_3]

[task3_1, task3_2, task3_3] >> task4
