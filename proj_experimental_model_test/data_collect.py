# Data Preparation Program
# - Extract data files from raw data
# - Process the data into clean datasets
# - Prepare data to be ready to be trained in a model

import pandas as pd
from pathlib import Path
from datetime import date

import data_source as ds

data_yr = date.today().year - 1 
year_start = 2018
year_end = data_yr - 2
year_validation = data_yr - 1

# Main Folder Path
main_folder = Path("proj_experimental_model_test")
# Raw Data Paths
path_data_raw = Path(main_folder / "data_raw")
path_crime_data_raw = Path(path_data_raw / "crime_data")

# Data Frames Collected
zhvi_dallas_county_df = None

zillow_sales_df = None
zillow_rent_df = None
zillow_listings_df = None
zillow_inventory_df = None

mortgage_rates_df = None
unemployment_rates_df = None
median_income_df = {}

school_rating_df = {}

crime_violent_df = {}
crime_property_df = {}


if path_data_raw.exists() and path_data_raw.is_dir():
    print("Raw data folder found.")
else:
    print("Missing data_raw folder. Download from Google Drive.")
    
# 1.PRICE MOMENTUM
## Get ZHVI from Zillow
zhvi_dallas_county_df = ds.get_zhvi_data() # Can calculate lag, growth, volatility from this data

# 2.SUPPLY AND DEMAND
zillow_sales_df = ds.get_zillow_supply('sales_count') # metro-lvl
zillow_rent_df = ds.get_zillow_supply('rent') # zip-lvl
zillow_listings_df = ds.get_zillow_supply('new_listings') # metro-lvl
zillow_inventory_df = ds.get_zillow_supply('inventory') # metro-lvl

# 3.ECONOMIC ENVIRONMENT

mortgage_rates_df = ds.get_mortgage_rates() # Weekly level (based on country)
unemployment_rates_df = ds.get_unemployment(data_yr) # County-lvl (Can try zip level if need be)

for year in range(year_start, data_yr):
    median_income_df[year] = ds.get_med_income(year)

    # 4.NEIGHBORHOOD QUALITY

    ## Get School Rating Data for every agency listed
    school_rating_df[year] = ds.get_school_rating(year) 

    # 5.SAFETY
    ## Get Crime Data for every agency listed in "agency_city.csv"
    #   P = Property, V = Violent
    #   Get offense per 100k people, offense count, clearance count, population of jurisdiction.
    crime_violent_df[year] = ds.get_crimes_df(year,path_crime_data_raw,'V')
    crime_property_df[year] = ds.get_crimes_df(year,path_crime_data_raw,'P')

## Agencies (TX/Dallas County)
"""
if Path(path_crime_data_raw / f"fbi_data/fbi_agencies.csv").exists():
    crime_agencies = pd.read_csv(path_crime_data_raw / f"fbi_data/fbi_agencies.csv")
else:
    crime_agencies = ds.pull_fbi_agencies("TX")

"""


"""
## Hardcode to test is dallas_crime_raw csv exists
if Path(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv").exists():
    crime_df_dallas = pd.read_csv(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv")
else:
    crime_df_dallas = ds.pull_dallas_crime(data_yr)
"""

# 6.SEASON/CLIMATE/Quarter
## Since all data already come with dates, we do not need to 'collect' months/quarters.

## If want to see what each dataframe as, print it (just print the head, since df can have too large values)
"""
print(zhvi_dallas_county_df.head())

print(zillow_sales_df.head())
print(zillow_rent_df.head())
print(zillow_listings_df.head())
print(zillow_inventory_df.head())

print(mortgage_rates_df.head())
print(unemployment_rates_df.head())

median_income_df[2018]
...
median_income_df[2025]


school_rating_df[2018]
... (Skip 2020, 2021 : Both will use 2019 data, since texas did not release school score data during this period[covid])
school_rating_df[2025]

print(crime_violent_df.head())
print(crime_property_df.head())
"""