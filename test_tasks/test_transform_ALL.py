import json
import os
import pandas as pd 

def transform_all_data_into_csv(n_files=None, filename='fulldata.csv'):
    dfs = []
    parent_folder = '/Users/pjost/GitHub/Datascientest_DE/Sprint_09/Evaluation_Sprint_09/Evaluation_Airflow/raw_files'
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

    df.to_csv(os.path.join('/Users/pjost/GitHub/Datascientest_DE/Sprint_09/Evaluation_Sprint_09/Evaluation_Airflow/clean_data', filename), index=False)

transform_all_data_into_csv()

