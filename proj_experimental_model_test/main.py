import sys
import data_source as ds
import data_normalize as dn

from pathlib import Path
from RealEstateData import RealEstateDataClass

if __name__ == "__main__":
    user_input = 0
    data_class = RealEstateDataClass(ds, 2018)
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
        "3. Get campus_zip data\n" \
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

                data_class.process_data(dn,main_path)

                print("Data finished processing. Check data_proc folder.")
            case '3':
                df = ds.get_campus_zip_data()
                print(df.head())
            case 'q':
                print("Quitting Program.")

        print("__________________________________________\n*" )