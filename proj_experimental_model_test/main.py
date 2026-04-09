import sys

import data_source as ds
import data_normalize as dn
import data_engineering as de
import model as mo
import matplotlib.pyplot as plt

from pathlib import Path
from RealEstateData import RealEstateDataClass

if __name__ == "__main__":
    user_input = 0
    data_class = RealEstateDataClass(ds, dn, de, year_earliest=2015)
    current_file_path = Path(__file__).resolve()
    main_path = current_file_path.parent
    models_path = main_path / "saved_models"
    models_path.mkdir(exist_ok=True)  # create models folder if doesnt exist
    model_log_path = main_path /"saved_models/model_logs.txt"
    
    target_cutoffs = {
        'target_zhvi_3m_pct':2022,
        'target_zhvi_6m_pct':2022,
        'target_zhvi_3m':2023,
        'target_zhvi_6m':2023
    }

    feature_split = {} # key: target, value: (X_train, X_test, y_train, y_test)
    models_trained = {} # store trained models in a dictionary

    print("__________________________________________\n" \
        "Welcome to Real Estate Market Analyzer\n" \
        "__________________________________________")
    
    while(user_input != 'q'):
        print("*\n__________________________________________\n" \
        "MENU OPTIONS (select by typing number)\n" \
        "__________________________________________\n" \
        " - Data Collection - \n" \
        "1. Collect Data\n" \
        "2. Process Raw Data\n" \
        "3. Collect Processed Data From Files\n" \
        "4. Build Features Vectors\n" \
        
        "\n - Model Training/Testing - \n" \
        "5. Train Models (3 month, 6 month)\n" \
        "6. Load Saved Models\n" \
        "7. Evaluate Models\n" \
        "8. Tune Hyperparameters (Incomplete)\n" \
        
        "\n - Debugging - \n" \
        "9. Get Redfin. \n" \
        "10. Get x_train head.\n" \
        "q. Quit Program. ")

        user_input = input("Enter the menu option number: ")
        print("__________________________________________")

        match(user_input):
            # data commands
            case '1':
                path_data_raw = main_path / "data_raw"
                if path_data_raw.exists() and path_data_raw.is_dir():
                    print("Raw data folder found.")
                else:
                    print("Missing data_raw folder. Download from Google Drive.")
                
                print("Data loading.")

                data_class.load_data()

                print("Data finished loading.")
            case '2':
                if data_class.zhvi_df is None:
                    print("No raw data loaded. Run option 1 first.")
                else:
                    print("Processing data...")
                    data_class.process_data(main_path)
                    print("Processing complete. Check data_proc folder.")

            case '3':
                print("Collecting processed data from data_proc folder.")
                data_class.get_processed_data(main_path)
                print("Processed Data Collected.")
            case '4':
                if data_class.zhvi_proc is None:
                    print("No processed data found. Run option 2 or 3 first.")
                else:
                    print("Building feature vectors...")
                    data_class.build_features(main_path)
                    print("Features built.")

            # model commands
            case '5':
                # Train models, put in models_trained
                target_names = ['target_zhvi_3m_pct','target_zhvi_6m_pct','target_zhvi_3m','target_zhvi_6m']
                mo.clear_log(model_log_path)
                ## train for all of the above
                for target in target_names:
                    print(f"Training for {target}.")
                    x_train, x_test, y_train, y_test = data_class.get_model_inputs(target, target_cutoffs[target])

                    model = mo.train_model(x_train,y_train) 

                    models_trained[target] = model
                    feature_split[target] = (x_train, x_test, y_train, y_test)

                    print("---")
                    # results = mo.eval_model(model, x_train, y_train, target, "Training")
                    # mo.write_log(model_log_path, results)
                    results = mo.eval_model(model, x_test, y_test, target, "Testing")
                    mo.write_log(model_log_path, results)
                    print("---")
                    results = mo.feature_analyze(model, x_train.columns.tolist())
                    mo.write_log(model_log_path, results)
                    print("---")
                    mo.save_model(model, models_path / f"{target}_rf_model.joblib")

                print(f"All models trained and saved to '{models_path}'.")

            case '6':
                # Load Model from path 
                targets = ['target_zhvi_3m_pct','target_zhvi_6m_pct','target_zhvi_3m','target_zhvi_6m']
                for target in targets:
                    model_path = models_path / f"{target}_rf_model.joblib"
                    if model_path.exists():
                        models_trained[target] = mo.load_model(model_path)
                        print(f"Loaded: {target}")
                    else:
                        print(f"Not found: {target} — train first with option 5")

                print("Models loaded.")

            case '7':
                # Evaluate models
                if not models_trained:
                    print("No trained models, train models first or load from memory (5 or 6)")
                else:
                    for target, model in models_trained.items():
                        if target in feature_split:
                            x_train, x_test, y_train, y_test = feature_split[target]
                        else:
                            x_train, x_test, y_train, y_test = data_class.get_model_inputs(target_str=target)
                            feature_split[target] = (x_train, x_test, y_train, y_test)

                        mo.eval_model(model, x_test, y_test, target)
                        mo.feature_analyze(model, x_train.columns.tolist())
                        

            case '8':
                # TODO: Tune Models
                # Train models, put in models_trained
                target_names = ['target_zhvi_3m_pct','target_zhvi_6m_pct','target_zhvi_3m','target_zhvi_6m']
                mo.clear_log(model_log_path)
                ## train for all of the above
                for target in target_names:
                    print(f"Training for {target}.")
                    x_train, x_test, y_train, y_test = data_class.get_model_inputs(target, target_cutoffs[target])

                    if target_names=='target_zhvi_3m_pct' or target_names=='target_zhvi_6m_pct':
                        model, params = mo.tune_model(x_train,y_train,param_type="pct")
                    else:
                        model, params = mo.tune_model(x_train,y_train,param_type="abs")

                    models_trained[target] = model
                    feature_split[target] = (x_train, x_test, y_train, y_test)

                    print("---")
                    results = mo.eval_model(model, x_train, y_train, target, "Training")
                    mo.write_log(model_log_path, results)
                    results = mo.eval_model(model, x_test, y_test, target, "Testing")
                    mo.write_log(model_log_path, results)
                    print("---")
                    results = mo.feature_analyze(model, x_train.columns.tolist())
                    mo.write_log(model_log_path, results)
                    print("---")
                    mo.save_model(model, models_path / f"{target}_rf_model.joblib")

                print(f"All models trained and saved to '{models_path}'.")    

            case '9':
                # Get Redfin data for debug purpose
                df_redfin = ds.get_redfin()     
                print(df_redfin.columns.tolist())      
                print(df_redfin.head())
            case '10':
                # print split heads
                target_names = ['target_zhvi_3m_pct','target_zhvi_6m_pct','target_zhvi_3m','target_zhvi_6m']
                for target in target_names:
                    x_train, x_test, y_train, y_test = data_class.get_model_inputs(target, target_cutoffs[target])
                print(x_train.head())
            case 'q':
                print("Quitting Program.")

        print("__________________________________________\n*" )