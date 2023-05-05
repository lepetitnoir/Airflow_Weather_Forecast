
import pandas as pd
pd.options.mode.chained_assignment = None
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump


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

    X, y = prepare_data()

    if lr_score > dt_score and lr_score > rf_score:
        train_and_save_model(LinearRegression(), X, y, path_to_model = './app/best_model.pckl')
    elif dt_score > lr_score and dt_score > rf_score:
        train_and_save_model(DecisionTreeRegressor(), X, y, path_to_model = './app/best_model.pckl')
    else:
        train_and_save_model(RandomForestRegressor(), X, y, path_to_model = './app/best_model.pckl')
    return 



def prepare_data():
    # reading data
    df = pd.read_csv('/app/clean_data/fulldata.csv')
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


def train_and_save_model(model, X, y, path_to_model):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)