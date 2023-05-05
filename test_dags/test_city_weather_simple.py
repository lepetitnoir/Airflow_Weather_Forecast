from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import datetime as dt
from datetime import timedelta
from joblib import dump
import json
import os
import pandas as pd
pd.options.mode.chained_assignment = None
import requests
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeRegressor

def get_city():
    address = "api.openweathermap.org/data/2.5/weather?q="
    key = "&appid=1e3e215327b663856b574dc1fc11fb70"
    weather = []
    r1 = requests.get(
        url="https://{address}london{key}".format(address=address, key=key)
        )
    weather.append(r1.json())
    r2 = requests.get(
        url="https://{address}paris{key}".format(address=address, key=key)
        )
    weather.append(r2.json())
    r3 = requests.get(
        url="https://{address}washington{key}".format(address=address, key=key)
        )
    weather.append(r3.json()) 
    now = dt.datetime.now().isoformat(timespec='minutes').replace("T","_")
    with open("/app/raw_files/{now}.json".format(now=now), "w") as outfile:
        json.dump(weather, outfile)
    return

def transform_data_into_csv_20(n_files=20, filename='data.csv'):

    dfs = []
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]
    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )
    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('/app/clean_data', filename), index=False)

def transform_all_data_into_csv(n_files=None, filename='fulldata.csv'):

    dfs = []
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]
    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )
    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('/app/clean_data', filename), index=False)

def prepare_data():
    # reading data
    df = pd.read_csv(path_to_data = '/app/clean_data/fulldata.csv')
    # ordering data according to city and date
    df.sort_values(['city', 'date'], ascending=True)
    dfs = []
    for c in df['city'].unique():
        df_temp = df[df['city'] == c]
        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)
        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)
        # deleting null values
        df_temp = df_temp.dropna()
        dfs.append(df_temp)
    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )
    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)
    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)
    #creating final features/target
    features = df_final.drop(['target'], axis=1)
    target = df_final['target']
    return features, target

X, y = prepare_data()

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    score = cross_validation.mean()
    return score

def linreg_score(task_instance):
    lr_score = compute_model_score(LinearRegression(), X, y)
    task_instance.xcom_push(
        key="lr_model_score",
        value={
           lr_score
        }
    )
     
def dtree_score(task_instance):
    dt_score = compute_model_score(DecisionTreeRegressor(), X, y)
    task_instance.xcom_push(
        key="dt_model_score",
        value={
        dt_score
        }
    )

def randfor_score(task_instance):
    rf_score = compute_model_score(RandomForestRegressor(), X, y)
    task_instance.xcom_push(
        key="rf_model_score",
        value={
        rf_score
        }
    )

def train_and_save_model(model, X, y, path_to_model):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)


def compare_and_save(task_instance):
    lr_dict = task_instance.xcom_pull(
            key="lr_model_score",
            task_ids='linear_regression_score'
        )
    lr_score = lr_dict.get("lr_model_score")
    
    dt_dict = task_instance.xcom_pull(
            key="dt_model_score",
            task_ids='decision_tree_regression_score'
        )
    dt_score = dt_dict.get("dt_model_score")
    
    rf_dict = task_instance.xcom_pull(
            key="rf_model_score",
            task_ids='random_forrest_regression_score'
        )
    rf_score = rf_dict.get("rf_model_score")

    if lr_score > dt_score and lr_score > rf_score:
        train_and_save_model(LinearRegression(), X, y, path_to_model = './app/best_model.pckl')
    elif dt_score > lr_score and dt_score > rf_score:
        train_and_save_model(DecisionTreeRegressor(), X, y, path_to_model = './app/best_model.pckl')
    else:
        train_and_save_model(RandomForestRegressor(), X, y, path_to_model = './app/best_model.pckl')
    return 

city_weather_dag = DAG( 
    dag_id='city_weather_dag',
    description='Querying weather in London, Paris, Washington',
    tags=['datascientest', 'tutorial'],
    schedule_interval=timedelta(minutes=1),
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
