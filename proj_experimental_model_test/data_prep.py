# Data Preparation Program
# - Extract data files from raw data
# - Process the data into clean datasets
# - Prepare data to be ready to be trained in a model

import pandas as pd
from pathlib import Path
from datetime import date
from data_source import pull_dallas_crime

data_yr = date.today().year - 1 

# Main Folder Path
main_folder = Path("proj_experimental_model_test")
# Raw Data Paths
path_data_raw = Path(main_folder / "data_raw")
path_crime_data_raw = Path(path_data_raw / "crime_data")

if path_data_raw.exists() and path_data_raw.is_dir():
    print("Raw data folder found.")
else:
    print("Missing data_raw folder. Download from Google Drive.")
    
## Hardcode to test is dallas_crime_raw csv exists
if Path(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv").exists():
    crime_df_dallas = pd.read_csv(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv")
else:
    crime_df_dallas = pull_dallas_crime(data_yr)

print(crime_df_dallas.head())