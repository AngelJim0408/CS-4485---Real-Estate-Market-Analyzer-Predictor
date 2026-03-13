# Data Preparation Program
# - Extract data files from raw data
# - Process the data into clean datasets
# - Prepare data to be ready to be trained in a model

import pandas as pd
from pathlib import Path
from datetime import date

import data_source as ds
import city_config

data_yr = date.today().year - 1 
year_start = 2018

# Main Folder Path
main_folder = Path("proj_experimental_model_test")
# Raw Data Paths
path_data_raw = Path(main_folder / "data_raw")
path_crime_data_raw = Path(path_data_raw / "crime_data")

if path_data_raw.exists() and path_data_raw.is_dir():
    print("Raw data folder found.")
else:
    print("Missing data_raw folder. Download from Google Drive.")
    
"""
Prepare Crime Data Sources, Pulled from Cities in Dallas County
"""
## Agencies (TX/Dallas County)
"""
if Path(path_crime_data_raw / f"fbi_data/fbi_agencies.csv").exists():
    crime_agencies = pd.read_csv(path_crime_data_raw / f"fbi_data/fbi_agencies.csv")
else:
    crime_agencies = ds.pull_fbi_agencies("TX")

"""
if Path(path_crime_data_raw / f"crime_violent_{data_yr}.csv").exists():
    crime_violent_df = pd.read_csv(path_crime_data_raw / f"crime_violent_{data_yr}.csv")
else:
    agency_city = pd.read_csv(path_data_raw / "tables/agency_city.csv")
    crime_violent_df = pd.DataFrame(columns=["agency","month","offenses_per_100k","offenses","clearances","population"])

    for row in agency_city.itertuples():
        crime_v_agency_df = ds.pull_crime_by_agency(row.agency_id,row.agency_name,data_yr,"V")
        if crime_violent_df.empty:
            crime_violent_df = crime_v_agency_df
        else:
            crime_violent_df = pd.concat([crime_violent_df, crime_v_agency_df], ignore_index=True)

    crime_violent_df.to_csv(path_crime_data_raw / f"crime_violent_{data_yr}.csv", index=False)


## Hardcode to test is dallas_crime_raw csv exists
if Path(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv").exists():
    crime_df_dallas = pd.read_csv(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv")
else:
    crime_df_dallas = ds.pull_dallas_crime(data_yr)

print(agency_city.head())