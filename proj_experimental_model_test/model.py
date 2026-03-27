import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
import joblib # to save model

def train_model(x_train, y_train, **kwargs):
    """
    Train the model using random forest
    """
    params = {"n_estimators":300,"max_depth":12,"min_samples_leaf": 5,"n_jobs":-1,"random_state":42,} # default params
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
