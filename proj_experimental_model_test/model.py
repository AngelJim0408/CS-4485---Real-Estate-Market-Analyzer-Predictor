import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit,RandomizedSearchCV
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
import joblib # to save model

def train_model(x_train, y_train, **kwargs):
    """
    Train the model using random forest
    """
    params = {"n_estimators":500,"max_depth":12,"min_samples_leaf": 5,"n_jobs":-1,"random_state":42,} # default params
    params.update(kwargs) # use kwargs if want to customize params 

    rf_model = RandomForestRegressor(**params)
    rf_model.fit(x_train,y_train)
    return rf_model

def eval_model(model, x_test, y_test, target_name: str, eval_type: str):
    """
    Evaluate model
    """
    y_predict = model.predict(x_test)

    mae = mean_absolute_error(y_test,y_predict)
    rmse = root_mean_squared_error(y_test,y_predict)
    r2 = r2_score(y_test,y_predict)

    eval_str = "\n---------------------"
    eval_str += f"\n-- {target_name} {eval_type} Evaluation --"
    if 'pct' in target_name:
        eval_str += f"\nMAE: {mae:,.4f}%"
        eval_str += f"\nRMSE:  {rmse:,.4f}%"
        
        dir_acc = np.mean(np.sign(y_predict) == np.sign(y_test))
        eval_str += f"\nDirectional Accuracy: {dir_acc:.3f}"
    else:
        eval_str += f"\nMAE: ${mae:,.0f}"
        eval_str += f"\nRMSE:  ${rmse:,.0f}"

    eval_str += f"\nR^2 score:   {r2:.3f}"
    eval_str += "\n---------------------"
    
    print(eval_str)

    return eval_str

def tune_model(x_train, y_train, n_iter=25, n_splits=3, random_state=42, param_type="pct"):
    tscv = TimeSeriesSplit(n_splits=n_splits)

    rf = RandomForestRegressor(
        n_jobs=-1,
        random_state=random_state
    )

    # Separate param grids for pct vs absolute models
    if param_type is "pct":
        param_dist= {
            "n_estimators":      [200, 300, 500, 800],
            "max_depth":         [8, 10, 12, 16, None],
            "min_samples_split": [2, 5, 10, 20],
            "min_samples_leaf":  [3, 5, 10, 15, 20],
            "max_features":      ["sqrt", "log2", 0.3, 0.5, 0.8],
        }
    else:
        param_dist = {
            "n_estimators":      [200, 300, 500],
            "max_depth":         [6, 8, 10, 12],      
            "min_samples_split": [10, 20, 50],         
            "min_samples_leaf":  [10, 20, 30, 50],     
            "max_features":      ["sqrt", 0.3, 0.5],
        }

    search = RandomizedSearchCV(
        estimator=rf,
        param_distributions=param_dist,
        n_iter=n_iter,
        cv=tscv,
        scoring="neg_root_mean_squared_error",  # regression
        verbose=1,
        random_state=random_state,
        n_jobs=-1,
        return_train_score  = True, # train vs cv
    )

    search.fit(x_train, y_train)

    print("\nBest Score (CV RMSE):", -search.best_score_)
    print("Best Params:", search.best_params_)

    # Show top 5 param combinations so you can see what's close
    results_df = pd.DataFrame(search.cv_results_)
    results_df = results_df[[
        "mean_test_score", "std_test_score",
        "mean_train_score",
        "param_n_estimators", "param_max_depth",
        "param_min_samples_leaf", "param_min_samples_split",
        "param_max_features"
    ]].copy()
    results_df["mean_test_score"]  = -results_df["mean_test_score"]
    results_df["mean_train_score"] = -results_df["mean_train_score"]
    results_df["overfit_gap"] = results_df["mean_test_score"] - results_df["mean_train_score"]
    results_df = results_df.sort_values("mean_test_score")

    print(f"\n── Top 5 parameter combinations ──")
    print(results_df.head(5).to_string(index=False))

    # Overfit test
    best_train = results_df.iloc[0]["mean_train_score"]
    best_test  = results_df.iloc[0]["mean_test_score"]
    gap        = results_df.iloc[0]["overfit_gap"]


    print(f"\nBest combo — Train RMSE: {best_train:.4f}  CV RMSE: {best_test:.4f}  Gap: {gap:.4f}")
    if gap > best_test * 0.3:
        print("Large train/CV gap detected ~= overfit")
        print("Consider: higher min_samples_leaf, lower max_depth, lower max_features")
    else:
        print("train/CV gap is acceptable")

    return search.best_estimator_, search.best_params_

def save_model(model, target_path):
    models_path = target_path.parent
    # save model to path (recommend save to saved_models folder)
    joblib.dump(model, target_path)
    return

def load_model(target_path):
    # get model from input path
    return joblib.load(target_path)

# model_analyze for debugging only
def model_analyze(model, x_test, y_test, x_train, target_name):
    preds = model.predict(x_test)
    residuals = y_test.values - preds

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f"Model Analysis — {target_name}")

    # Plot 1: Actual vs Predicted 
    axes[0].scatter(y_test, preds, alpha=0.3, s=10)
    axes[0].plot(
        [y_test.min(), y_test.max()],
        [y_test.min(), y_test.max()],
        'r--', linewidth=1.5
    )
    axes[0].set_xlabel("Actual ZHVI")
    axes[0].set_ylabel("Predicted ZHVI")
    axes[0].set_title("Actual vs Predicted")

    #  Plot 2: Residuals vs Predicted 
    # Should be randomly scattered around 0
    # Patterns here = systematic errors
    axes[1].scatter(preds, residuals, alpha=0.3, s=10)
    axes[1].axhline(y=0, color='r', linestyle='--', linewidth=1.5)
    axes[1].set_xlabel("Predicted ZHVI")
    axes[1].set_ylabel("Residual (Actual - Predicted)")
    axes[1].set_title("Residuals vs Predicted")

    # Plot 3: Feature importance 
    importance_df = (
        pd.DataFrame({
            "feature":    x_train.columns,
            "importance": model.feature_importances_
        })
        .sort_values("importance", ascending=True)
        .tail(20)  # top 20
    )
    axes[2].barh(importance_df["feature"], importance_df["importance"])
    axes[2].set_xlabel("Importance")
    axes[2].set_title("Top 20 Feature Importances")

    plt.tight_layout()
    plt.show()

def feature_analyze(model, features: list, top_n: int = 20):
    """
    get top N most important features as a DataFrame.
    """
    importance_df = pd.DataFrame({"feature":features,"pct_infl": model.feature_importances_}).sort_values("pct_infl", ascending=False)

    feature_results = f"\n-- Top {top_n} features --\n"
    feature_results += importance_df.head(top_n).to_string(index=False)
    feature_results += "\n---------------------\n"

    return feature_results

def write_log(logfile, content: str):
    with open(logfile,'a') as log:
        log.write(content)

def clear_log(logfile):
    with open(logfile, 'w') as log:
        pass
