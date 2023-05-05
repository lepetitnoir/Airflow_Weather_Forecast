import pandas as pd

path_to_data = '/Users/pjost/GitHub/Datascientest_DE/Sprint_09/Evaluation_Sprint_09/Evaluation_Airflow/clean_data/fulldata1.csv'

def prepare_data():
    # reading data
    df = pd.read_csv(path_to_data)
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
    
data = prepare_data()

print(data)
