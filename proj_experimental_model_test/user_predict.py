import pandas as pd
import data_engineering as de
import joblib

from datetime import date
from pathlib import Path

from model import load_model
from database import RealEstateDB


def get_user_inputs(usr_input_str):
    """Parse 'zipcode,year,month' — year and month are optional."""
    split   = usr_input_str.split(",")
    zipcode = split[0].strip().zfill(5)
    year    = int(split[1].strip()) if len(split) > 1 and split[1].strip() else None
    month   = int(split[2].strip()) if len(split) > 2 and split[2].strip() else None
    return zipcode, year, month

def load_models(m3_path, m6_path, m3_pct_path, m6_pct_path):
    models_absolute = {
        "3m":  joblib.load(m3_path),
        "6m":  joblib.load(m6_path),
    }
    models_percent = {
        "3m_pct": joblib.load(m3_pct_path),
        "6m_pct": joblib.load(m6_pct_path),
    }
    return models_absolute, models_percent

def get_predictions(master_df, zipcode, year, month, models_abs, models_pct):
    """Build feature vectors for the requested row and return predictions."""
    zip_history = master_df[master_df["zipcode"] == zipcode]
    if zip_history.empty:
        print(f"No data found for zipcode: {zipcode}")
        return None

    feature_vectors = de.create_feature_vectors(master_df)
    zip_vectors     = feature_vectors[feature_vectors['zipcode'] == zipcode]

    if year is not None and month is not None:
        target_rows = zip_vectors[(zip_vectors['year'] == year) & (zip_vectors['month'] == month)]
    elif year is not None:
        target_rows = zip_vectors[zip_vectors['year'] == year].tail(1)
    elif month is not None:
        target_rows = zip_vectors[zip_vectors['month'] == month].tail(1)
    else:
        target_rows = zip_vectors.tail(1)

    if target_rows.empty:
        print(f"No feature data for zipcode {zipcode}, {month}/{year}.")
        return None

    year  = target_rows['year'].iloc[0]
    month = target_rows['month'].iloc[0]

    feat        = de.clean_features_predict(target_rows)
    predictions = {}

    print("Getting predictions...")
    for model_type, model in {**models_abs, **models_pct}.items():
        predictions[model_type] = model.predict(feat)[0]

    return predictions, year, month

def get_zhvi_curr(master_df, zipcode, year, month):
    row = master_df[
        (master_df["zipcode"] == zipcode) &
        (master_df["year"]    == year) &
        (master_df["month"]   == month)
    ]
    return row.iloc[0].get("zhvi", None) if not row.empty else None

def get_market_signals(master_df, zipcode, year, month):
    row = master_df[
        (master_df["zipcode"] == zipcode) &
        (master_df["year"]    == year) &
        (master_df["month"]   == month)
    ]
    if row.empty:
        return {}, {}

    r = row.iloc[0]
    market_factors = {
        "Mortgage Rate":     r.get("mortgage_rate",     "N/A"),
        "Unemployment Rate": r.get("unemployment_rate", "N/A"),
        "Median Income":     r.get("median_income",     "N/A"),
        "Inventory":         r.get("inventory",         "N/A"),
        "Sales Count":       r.get("sales_count",       "N/A"),
        "New Listings":      r.get("new_listings",      "N/A"),
    }
    add_factors = {
        "Average School Rating (0-100)": r.get("school_rating_mean",        "N/A"),
        "Violent Crime Per 100k":        r.get("violent_offenses_per_100k",  "N/A"),
        "Property Crime Per 100k":       r.get("property_offenses_per_100k", "N/A"),
    }
    return market_factors, add_factors

def print_zips(db: RealEstateDB):
    """Print all zipcodes from the database."""
    zips = db.get_zipcodes()
    print("_____________* Zipcodes *_____________")
    for z in zips:
        print(z)
    print("______________________________________")

def print_features(df):
    feature_vectors = de.create_feature_vectors(df)
    cols = feature_vectors.columns.tolist()
    print("_____________* Features *_____________")
    for col in cols:
        print(col)
    print(f"Feature count: {len(cols)}")
    print("______________________________________")

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

    if pred_3 is not None and current_zhvi:
        pct = (pred_3 - current_zhvi) / current_zhvi * 100
        print(f"3-Month Prediction: ${pred_3:>10,.0f}   ({'+' if pct>=0 else ''}{pct:.2f}%)")
    if pred_6 is not None and current_zhvi:
        pct = (pred_6 - current_zhvi) / current_zhvi * 100
        print(f"6-Month Prediction: ${pred_6:>10,.0f}   ({'+' if pct>=0 else ''}{pct:.2f}%)")

    if market_factors:
        print("\n Market Information:")
        for label, val in market_factors.items():
            if val != "N/A":
                print(f"    {label:<25} {val}")

    if add_factors:
        print("\n Additional Factors:")
        for label, val in add_factors.items():
            if val != "N/A":
                try:
                    print(f"    {label:<35} {float(val):.2f}")
                except (TypeError, ValueError):
                    print(f"    {label:<35} {val}")


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------
current_file_path = Path(__file__).resolve()
main_path         = current_file_path.parent
db_path           = main_path / "real_estate.db"

model_3m_path     = main_path / "saved_models/target_zhvi_3m_rf_model.joblib"
model_6m_path     = main_path / "saved_models/target_zhvi_6m_rf_model.joblib"
model_3m_pct_path = main_path / "saved_models/target_zhvi_3m_pct_rf_model.joblib"
model_6m_pct_path = main_path / "saved_models/target_zhvi_6m_pct_rf_model.joblib"

models_abs, models_pct = load_models(model_3m_path, model_6m_path, model_3m_pct_path, model_6m_pct_path)

# Load master DataFrame directly from the database
db        = RealEstateDB(db_path)
master_df = de.get_master_df(db_path)
master_df["zipcode"] = master_df["zipcode"].astype(str).str.zfill(5)

print("\n--- User Prediction Tool ---")
usr_input = None
while usr_input != 'q':
    print(
        'What would you like to do?\n---'
        '\n1. Predict future home values (zip, year, month)'
        '\n2. Show feature list'
        '\n3. Show available zipcodes'
        '\nq. Quit.'
    )
    print("___")
    usr_input = input("Enter Number: ")

    match usr_input:
        case '1':
            print(
                "\n-------------------------------\n"
                "Format: zipcode,year,month  (year and month optional)\n"
                "Example: 75040,2025,4\n"
                "--------"
            )
            raw = input("Enter: ").strip()
            zipcode, year, month = get_user_inputs(raw)

            result = get_predictions(master_df, zipcode, year, month, models_abs, models_pct)
            if result is None:
                print("No predictions could be generated.")
                continue

            predictions, year, month = result
            curr_zhvi = get_zhvi_curr(master_df, zipcode, year, month)
            market_factors, add_factors = get_market_signals(master_df, zipcode, year, month)
            print_results(zipcode, year, month, curr_zhvi, predictions, market_factors, add_factors)
            print("-------------------------------")

        case '2':
            print_features(master_df)

        case '3':
            print_zips(db)

        case 'q':
            print("Quitting...")
            db.close()

        case _:
            print(f"Unknown input: {usr_input}")