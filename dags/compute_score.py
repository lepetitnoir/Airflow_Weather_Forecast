
def compute_model_score(model: str, X, y) -> None: # numpy or pandas for array, abbreviation for array
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import cross_val_score
    from sklearn.tree import DecisionTreeRegressor
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    score = cross_validation.mean()
    return score
