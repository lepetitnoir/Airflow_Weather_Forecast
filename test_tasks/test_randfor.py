import pandas as pd
pd.options.mode.chained_assignment = None
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

path_to_data = '/Users/pjost/GitHub/Datascientest_DE/Sprint_09/Evaluation_Sprint_09/Evaluation_Airflow/clean_data/fulldata1.csv'

path_to_model = '/Users/pjost/GitHub/Datascientest_DE/Sprint_09/Evaluation_Sprint_09/Evaluation_Airflow/test_tasks/rf_model.pckl'

model = RandomForestRegressor()

def randfor_score():
    X, y = prepare_data()
    train_and_save_model(X, y)
    return compute_model_score(X, y)

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


def train_and_save_model(X, y):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)

def compute_model_score(X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    score_rf = cross_validation.mean()
    return score_rf

score = randfor_score()

print(score)