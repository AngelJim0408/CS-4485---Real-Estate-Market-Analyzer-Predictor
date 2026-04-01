import pandas as pd
import data_engineering as de
import joblib

from datetime import date
from pathlib import Path

from model import load_model

def get_user_inputs(usr_input_str):
    ## Find given information in master dataframe from user zip, year, month
    split = usr_input_str.split(",")

    zipcode = split[0].strip().zfill(5)

    # check if any more values after split.
    year = int(split[1]) if len(split) > 1 and split[1] else None
    month = int(split[2]) if len(split) > 2 and split[2] else None

    return zipcode, year, month

def load_models(m3_path,m6_path,m3_pct_path,m6_pct_path):
    models_absolute = {
        "3m" : joblib.load(m3_path),
        "6m" : joblib.load(m6_path),
    }
    models_percent = {
        "3m_pct" : joblib.load(m3_pct_path),
        "6m_pct" : joblib.load(m6_pct_path),
    }
    return models_absolute, models_percent

def get_predictions(master_df, zipcode, year, month, models_abs, models_pct):
    # Get entire history of selected zipcode
    zip_history = master_df[master_df["zipcode"] == zipcode].copy()

    if len(zip_history) == 0:
        print(f"No data found for zipcode: {zipcode}")
        return None
    
    feature_vectors = de.create_feature_vectors(master_df)

    # (feature vectors should already sorted)
    zip_vectors = feature_vectors[feature_vectors['zipcode'] == zipcode]
    if year is not None and month is not None:
        # if year/month given, use as filter 
        target_rows = zip_vectors[(zip_vectors['year'] == year) & (zip_vectors['month'] == month)] # filter by zip,year,month
    elif year is not None:
        # year given, month not gvien
        target_rows = zip_vectors[(zip_vectors['year'] == year)].tail(1)
    elif month is not None:
        # month given but not year
        target_rows = zip_vectors[(zip_vectors['month'] == month)].tail(1)
    else:
        target_rows = zip_vectors.tail(1)

    year = target_rows['year'].iloc[0]
    month = target_rows['month'].iloc[0]

    if target_rows.empty:
        print(f"No feature data for zipcode: {zipcode}, {month}/{year}.")
        print("Try a different date or check your dataset range.")
        return None
    
    # Filter to target AFTER getting correct features built and set, Then get predictions
    feat = de.clean_features_predict(target_rows)
    predictions = {}

    print("Getting predictions...")
    for model_type, model in models_abs.items():
        predictions[model_type] = model.predict(feat)[0]

    for model_type, model in models_pct.items():
        predictions[model_type] = model.predict(feat)[0]

    return predictions, year, month

# Output format functions
def get_zhvi_curr(master_df, zipcode, year, month):
    row = master_df[(master_df["zipcode"]==zipcode) & (master_df["year"]== year) & (master_df["month"]== month)]
    if row.empty:
        return None
    return row.iloc[0].get("zhvi", None) # first row

def get_market_signals(master_df, zipcode, year, month):
    # Pull economic feature values for display
    row = master_df[(master_df["zipcode"]==zipcode) & (master_df["year"]== year) & (master_df["month"]== month)]

    if row.empty:
        return {}
    
    r = row.iloc[0]
    """
    zipcode,year,month,zhvi,sales_count,new_listings,inventory,unemployment_rate,mortgage_rate,median_income,total_population,
    school_rating_mean,school_rating_max,school_count,violent_offenses,violent_clearances,violent_offenses_per_100k,property_offenses,
    property_clearances,property_offenses_per_100k
    """
    return {
        "Mortgage Rate":r.get("mortgage_rate", "N/A"),
        "Unemployment": r.get("unemployment_rate", "N/A"),
        "Median Income":r.get("median_income", "N/A"),
        "Inventory":    r.get("inventory", "N/A"),
        "Sales Count":  r.get("sales_count", "N/A"),
        "New Listings": r.get("new_listings", "N/A"),

    }, {
        "Aberage School Rating (0 - 100)": r.get("school_rating_mean", "N/A"),
        "Violent Crime Per 100k":     r.get("violent_offenses_per_100k", "N/A"),
        "Property Crime Per 100k":     r.get("property_offenses_per_100k", "N/A"),
    }

def print_results(zipcode, year, month, current_zhvi, predictions, market_factors, add_factors):
    print("__________________________________________________________")
    print(f"Real Estate Predictions for: {zipcode} | {month}/{year}")
    print("*________________________________________________________*")

    if current_zhvi:
        print(f"Current ZHVI:   ${current_zhvi:,.0f}")
    else:
        print(f"Current ZHVI:   N/A")

    pred_3 = predictions.get("3m")
    pred_6 = predictions.get("6m")
    #pred_3pct = predictions.get("3m_pct")
    #pred_6pct = predictions.get("6m_pct")
    
    pct_change_3pct = (pred_3 - current_zhvi) / current_zhvi * 100
    pct_change_6pct = (pred_6 - current_zhvi) / current_zhvi * 100

    if pred_3 is not None:
        sign = "+" if pct_change_3pct >= 0 else ""
        print(f"3-Month Prediction: ${pred_3:>10,.0f}   ({sign}{pct_change_3pct:.2f}%)")
    if pred_6 is not None:
        sign = "+" if pct_change_6pct >= 0 else ""
        print(f"6-Month Prediction: ${pred_6:>10,.0f}   ({sign}{pct_change_6pct:.2f}%)")

    if market_factors:
        print(f"\n Market Information:")
        for label, val in market_factors.items():
            if val != "N/A":
                print(f"    {label:<20} {val}")

    if add_factors:
        print("\n Additional Factors:")
        for label, val in add_factors.items():
            if val != "N/A":
                print(f"    {label:<30} {val:.2f}")
    return

curr_date = date.today()

current_file_path = Path(__file__).resolve()
main_path = current_file_path.parent

model_3m_path = main_path / "saved_models/target_zhvi_3m_rf_model.joblib"
model_6m_path = main_path / "saved_models/target_zhvi_6m_rf_model.joblib"
model_3m_pct_path = main_path / "saved_models/target_zhvi_3m_pct_rf_model.joblib"
model_6m_pct_path = main_path / "saved_models/target_zhvi_6m_pct_rf_model.joblib"

models_abs, models_pct = load_models(model_3m_path,model_6m_path,model_3m_pct_path,model_6m_pct_path)

master_df = de.get_master_df(main_path)
master_df["zipcode"] = master_df["zipcode"].astype(str).str.zfill(5) 

"""
Input -> Things involving features

Output ->
---- Real Estate Prediction : (zipcode) (month) (year) ----
 Current ZHVI: $342,000 
 
 3-Month Prediction: $348,500 (+1.9%) 
 6-Month Prediction: $355,200 (+3.8%) 
 
 Market Signals: 
 Mortgage Rate: 7.2% 
 Months of Supply: 2.1 (Seller's market) 
 Absorption Rate: 0.82 
 Unemployment: 4.1% 
 Direction Confidence: Rising (62% directional accuracy)
-----------------------------------------------------------
"""

print("\n--- User Input Testing ---")
usr_input = None
while(usr_input != 'q'):
    print('What type of information do you need?\n---' \
    '\n1. Make predictions of future home values in regions based on user input information (zip, month, year)' \
    '\n2. Request future predictions via user input (i.e. find predicted price of user input zipcode, month, year)' \
    '\nq. Quit Program.')
    print("___")
    usr_input = input("Enter Number Input: ")

    match(usr_input):
        case '1':
            print("\n-------------------------------\n" \
            "Enter zipcode,year,month in stated order separated by commas without any spaces.\n" \
            "--------")
            print("\n  Format: zipcode,year,month  (e.g. 75040,2025,4) year and month are optional")
            raw = input("Enter Information: ").strip()
            zipcode, year, month = get_user_inputs(raw)

            if zipcode is None:
                print("Zipcode does not exist in Dallas County.")
                continue

            # TODO: Handle month/ propagation for what we have.
            predictions, year, month = get_predictions(master_df, zipcode, year, month, models_abs, models_pct)

            if predictions is None:
                print("No predictions could be generated.")
                continue

            # TODO: check if any empty parts in feature, if there is, forward fill from previous month.
            # if no more previous month, error message and go back to original user input msg.

            curr_zhvi = get_zhvi_curr(master_df, zipcode, year, month)
            market_factors, add_factors = get_market_signals(master_df, zipcode, year, month)

            print_results(zipcode, year, month, curr_zhvi, predictions, market_factors, add_factors)
            print("-------------------------------")
        case '2':
            print("Not currently implemented.")
        case 'q':
            print("Quitting Program...")
        case _:
            print(f"No listed command associated with user input: {usr_input}") 