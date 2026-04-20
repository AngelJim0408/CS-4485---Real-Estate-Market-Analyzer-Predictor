import sys

import data_source as ds
import data_normalize as dn
import data_engineering as de
import model as mo

from pathlib import Path
from RealEstateData import RealEstateDataClass

if __name__ == "__main__":
    user_input = 0
    data_class = RealEstateDataClass(ds, dn, de, year_earliest=2011)
    current_file_path = Path(__file__).resolve()
    main_path   = current_file_path.parent
    models_path = main_path / "saved_models"
    models_path.mkdir(exist_ok=True)
    model_log_path = main_path / "saved_models/model_logs.txt"

    target_cutoffs = {
        'target_zhvi_3m':     2023,
        'target_zhvi_6m':     2023,
    }

    feature_split  = {}
    models_trained = {}

    print("__________________________________________\n"
          "Welcome to Real Estate Market Analyzer\n"
          "__________________________________________")

    while user_input != 'q':
        print(
            "*\n__________________________________________\n"
            "MENU OPTIONS (select by typing number)\n"
            "__________________________________________\n"
            " - Data Collection -\n"
            "1. Collect Data\n"
            "2. Process Raw Data\n"
            "3. Load Processed Data From Database\n"
            "4. Build Feature Vectors\n"
            "\n - Model Training/Testing -\n"
            "5. Train Models (3 month, 6 month)\n"
            "6. Load Saved Models\n"
            "7. Evaluate Models\n"
            "8. Tune Hyperparameters (Incomplete)\n"
            "\n - Info / Debugging -\n"
            "9. Show database table summary\n"
            "10. Get x_train head\n"
            "q. Quit Program."
        )

        user_input = input("Enter the menu option number: ")
        print("__________________________________________")

        match user_input:

            case '1':
                print("Loading data from sources (or database cache)...")
                data_class.load_data()
                print("Data finished loading.")

            case '2':
                if data_class.zhvi_df is None:
                    print("No raw data loaded. Run option 1 first.")
                else:
                    print("Processing data...")
                    data_class.process_data()
                    print("Processing complete. All processed data saved to database.")

            case '3':
                print("Loading processed data from database...")
                data_class.get_processed_data()
                print("Processed data loaded.")

            case '4':
                if data_class.zhvi_proc is None:
                    print("No processed data found. Run option 2 or 3 first.")
                else:
                    print("Building feature vectors...")
                    data_class.build_features()
                    print("Features built. Master DataFrame saved to database.")

            case '5':
                if data_class.master_df is None:
                    # check if we can get master dataframe first.
                    data_class.load_master_from_db()
                    if data_class.master_df is None:
                        print("No master DataFrame. Run option 4 first.")
                        continue
                # # Train models, put in models_trained (dont use pct for now ['target_zhvi_3m_pct','target_zhvi_6m_pct'])
                target_names = ['target_zhvi_3m','target_zhvi_6m']
                mo.clear_log(model_log_path)

                for target in target_names:
                    print(f"Training for {target}.")
                    x_train, x_test, y_train, y_test = data_class.get_model_inputs(
                        target, target_cutoffs[target]
                    )
                    model = mo.train_model(x_train, y_train)
                    models_trained[target] = model
                    feature_split[target]  = (x_train, x_test, y_train, y_test)

                    print("---")
                    results = mo.eval_model(model, x_test, y_test, target, "Testing")
                    mo.write_log(model_log_path, results)
                    print("---")
                    results = mo.feature_analyze(model, x_train.columns.tolist())
                    mo.write_log(model_log_path, results)
                    print("---")
                    mo.save_model(model, models_path / f"{target}_rf_model.joblib")

                print(f"All models trained and saved to '{models_path}'.")

            case '6':
                # Load Model from path (don't use for now -> ['target_zhvi_3m_pct','target_zhvi_6m_pct'])
                targets = ['target_zhvi_3m','target_zhvi_6m']
                for target in targets:
                    model_path = models_path / f"{target}_rf_model.joblib"
                    if model_path.exists():
                        models_trained[target] = mo.load_model(model_path)
                        print(f"Loaded: {target}")
                    else:
                        print(f"Not found: {target} — train first with option 5")
                print("Models loaded.")

            case '7':
                if not models_trained:
                    print("No trained models. Train (5) or load (6) first.")
                else:
                    for target, model in models_trained.items():
                        if target not in feature_split:
                            feature_split[target] = data_class.get_model_inputs(
                                target, target_cutoffs[target]
                            )
                        x_train, x_test, y_train, y_test = feature_split[target]
                        mo.eval_model(model, x_test, y_test, target, "Testing")
                        mo.feature_analyze(model, x_train.columns.tolist())

            case '8':
                if data_class.master_df is None:
                    print("No master DataFrame. Run option 4 first.")
                else:
                    target_names = ['target_zhvi_3m','target_zhvi_6m']
                    mo.clear_log(model_log_path)

                    for target in target_names:
                        print(f"Tuning for {target}.")
                        x_train, x_test, y_train, y_test = data_class.get_model_inputs(
                            target, target_cutoffs[target]
                        )
                        param_type = "pct" if "pct" in target else "abs"
                        model, params = mo.tune_model(x_train, y_train, param_type=param_type)

                        models_trained[target] = model
                        feature_split[target]  = (x_train, x_test, y_train, y_test)

                        print("---")
                        mo.write_log(model_log_path, mo.eval_model(model, x_train, y_train, target, "Training"))
                        mo.write_log(model_log_path, mo.eval_model(model, x_test,  y_test,  target, "Testing"))
                        mo.write_log(model_log_path, mo.feature_analyze(model, x_train.columns.tolist()))
                        print("---")
                        mo.save_model(model, models_path / f"{target}_rf_model.joblib")

                    print(f"All models tuned and saved to '{models_path}'.")

            case '9':
                # Show row counts for every table in the database
                ds.db.table_summary()

            case '10':
                if data_class.master_df is None:
                    print("No master DataFrame. Run option 4 first.")
                else:
                    target_names = ['target_zhvi_3m','target_zhvi_6m']
                    for target in target_names:
                        x_train, x_test, y_train, y_test = data_class.get_model_inputs(
                            target, target_cutoffs[target]
                        )
                    print(x_train.head())

            case 'q':
                print("Quitting Program.")
                ds.db.close()

        print("__________________________________________\n*")