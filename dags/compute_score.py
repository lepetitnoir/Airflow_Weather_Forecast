
def compute_model_score(
    model: "LinearRegression" | "DecisionTreeRegressor" | "RandomForestRegressor", 
    X, 
    y
) -> None: # type score?
    from sklearn.model_selection import cross_val_score
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    score = cross_validation.mean()
    return score
