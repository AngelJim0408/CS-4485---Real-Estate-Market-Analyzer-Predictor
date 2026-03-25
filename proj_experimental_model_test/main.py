import sys

import data_source as ds
import data_normalize as dn
import data_engineering as de

from pathlib import Path
from RealEstateData import RealEstateDataClass
from database import RealEstateDB

if __name__ == "__main__":
    user_input = 0
    data_class = RealEstateDataClass(ds, dn, de, 2018)
    current_file_path = Path(__file__).resolve()
    main_path = current_file_path.parent

    print("__________________________________________\n" \
        "Welcome to Real Estate Market Analyzer\n" \
        "__________________________________________")

    while(user_input != 'q'):
        print("*\n__________________________________________\n" \
        "MENU OPTIONS (select by typing number)\n" \
        "__________________________________________\n" \
        "1. Collect Data\n" \
        "2. Process Raw Data\n" \
        "3. Collect Processed Data From Files\n" \
        "4. Build Features for model\n" \
        "5. Load Processed Data into Database\n" \
        "q. Quit Program. ")

        user_input = input("Enter the menu option number: ")
        print("__________________________________________")

        match(user_input):
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
                print("Data processing.")
                (main_path / "data_proc").mkdir(exist_ok=True)
                data_class.process_data(main_path)
                print("Data finished processing. Check data_proc folder.")

            case '3':
                print("Collecting processed data from data_proc folder.")
                data_class.get_processed_data(main_path)
                print("Processed Data Collected.")

            case '4':
                data_class.build_features(main_path)

            case '5':
                print("Loading processed data into database.")
                db = RealEstateDB(main_path / "real_estate.db")
                db.create_tables()
                db.load_from_csvs(main_path) 
                db.table_summary()
                db.close()
                print("Database load complete. File: real_estate.db")

            case 'q':
                print("Quitting Program.")

        print("__________________________________________\n*")