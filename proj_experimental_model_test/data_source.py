# Data Source Program
# - Provide functionality to pull sources dynamically from web
# - Access and save raw data

## Preparing Crime Data
# www.dallasopendata.com
# - Police Incidents (Dallas PD): Dataset ID (Socrata): qv6i-rri7
# pip install sodapy

import os
import requests
import pandas as pd
from datetime import date
from sodapy import Socrata
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

FBI_API_KEY = os.getenv("FBI_API_KEY")

main_path = Path("proj_experimental_model_test")
url_fbi = "https://api.usa.gov/crime/fbi/cde"

# Base Worker functs
def writeWebToLocal(url, target_path):
    target_path.parent.mkdir(parents=True, exist_ok=True)

    req = requests.get(url)
    with open(target_path, "wb") as file:
        file.write(req.content)
# -------------

# 1. PRICE MOMENTUM DATA PULLING/GETTING
def pull_zhvi_data():
    target_path = main_path / "data_raw/price_momentum/zhvi_zipcode.csv"
    url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"

    writeWebToLocal(url, target_path)

    return pd.read_csv(target_path)

def get_zhvi_data():
    target_path = main_path / "price_momentum/zhvi_zipcode.csv"
    if Path(target_path).exists():
        zhvi_df = pd.read_csv(target_path)
    else:
        zhvi_df = pull_zhvi_data()
    # zhvi has all zipcodes, we clean to get just the ones in dallas county.
    dallas_zhvi_df = zhvi_df[zhvi_df["CountyName"] == "Dallas County"]
    return dallas_zhvi_df

# 2. SUPPLY DEMAND DATA PULLING/GETTING
def pull_zillow_sales():
    target_path = main_path / "data_raw/supply_demand/zillow_sales_count.csv"
    url = "https://files.zillowstatic.com/research/public_csvs/sales_count_now/Metro_sales_count_now_uc_sfrcondo_month.csv"

    writeWebToLocal(url, target_path)

    return pd.read_csv(target_path)

def pull_zillow_rent():
    target_path = main_path / "data_raw/supply_demand/zillow_rent.csv"
    url = "https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"

    writeWebToLocal(url, target_path)

    return pd.read_csv(target_path)

def pull_zillow_listings():
    target_path = main_path / "data_raw/supply_demand/zillow_new_listings.csv"
    url = "https://files.zillowstatic.com/research/public_csvs/new_listings/Metro_new_listings_uc_sfrcondo_sm_month.csv"
    writeWebToLocal(url, target_path)

    return pd.read_csv(target_path)

def pull_zillow_inv():
    target_path = main_path / "data_raw/supply_demand/zillow_inventory.csv"
    url = "https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv"
    writeWebToLocal(url, target_path)

    return pd.read_csv(target_path)

def get_zillow_supply(type):
    """
    types: sales_count, rent, new_listings, inventory
    """
    target_path = main_path / f"data_raw/supply_demand/zillow_{type}.csv"

    if Path(target_path).exists():
        zillow_supplydemand_df = pd.read_csv(target_path)
    else:
        match type:
            case 'sales_count':
                zillow_supplydemand_df = pull_zillow_sales()
            case 'rent':
                zillow_supplydemand_df = pull_zillow_rent()
            case 'new_listings':
                zillow_supplydemand_df = pull_zillow_listings()
            case 'inventory':
                zillow_supplydemand_df = pull_zillow_inv()
            case _:
                return None
    # zhvi has all zipcodes, we clean to get just the ones in dallas county.
    #dallas_zhvi_df = zhvi_df[zhvi_df["CountyName"] == "Dallas County"]
    return zillow_supplydemand_df

def pull_fbi_agencies(state="TX", county_filter="DALLAS"):
    """
    Pull agencies from county

    
    """
    url_agencies = f"{url_fbi}/agency/byStateAbbr/{state}?API_KEY={FBI_API_KEY}"

    response = requests.get(url_agencies)
    response.raise_for_status() # check for success
    data = response.json()

    agencies = []
    for county_key, agency_list in data.items():
        if county_filter in county_key:  # includes single and multi-county agencies
            agencies.extend(agency_list)
    
    if agencies:
        results_df = pd.json_normalize(agencies)

        # Save to csv so we don't need to call api
        # create path if does not exist
        output_path = Path(main_path / "data_raw/crime_data/fbi_data")
        output_path.mkdir(parents=True, exist_ok=True)

        results_df.to_csv(output_path  / f"fbi_agencies_{county_filter}.csv", index=False, lineterminator="\n")
    else:
        results_df = pd.DataFrame()

    return results_df

def pull_crime_by_agency(agency_ori, agency_name, year, offense="V"):
    """
    Default for when cities don't provide own data. Yearly crime incident reports (NIBRS)
    - pulls fbi crime data by agency
    - may need to change to get monthly reports
    """
    url_summary = f"{url_fbi}/summarized/agency/{agency_ori}/{offense}?"
    params = {'from' : f'01-{year}', 'to' : f'12-{year}', 'API_KEY' : f'{FBI_API_KEY}'}

    response = requests.get(url_summary, params=params)
    response.raise_for_status()
    data = response.json()

    if data:
        rows = []

        offense_per_100k = data["offenses"]["rates"][f"{agency_name} Offenses"]
        offenses = data["offenses"]["actuals"][f"{agency_name} Offenses"]
        clearances = data["offenses"]["actuals"][f"{agency_name} Clearances"]
        population = data["populations"]["population"][f"{agency_name}"]

        for month in offenses:
            rows.append({
                "agency": agency_ori,
                "month": month,
                "offenses_per_100k" : offense_per_100k[month],
                "offenses": offenses[month],
                "clearances": clearances[month],
                "population": population[month]
            })
        results_df = pd.DataFrame(rows)
        return results_df
    else:
        return pd.DataFrame()

def pull_dallas_crime(year=None):
    """
    Pulls Police Incident Information from Dallas PD
    """
    # Crime Data: Finding data from dallasopendata (Police Incidents)
    # - website puts limit to 1000 rows each time.
    client = Socrata("www.dallasopendata.com", None)
    base_limit = 1000
    base_offset = 0
    batches = []

    if year is None:
        year = date.today().year - 1 # get data from prev year (since info for this year likely to not be complete)

    while True:
        batch_result = client.get("qv6i-rri7", 
                                select="date1,year1," \
                                "nibrs_crime,nibrs_crime_category,nibrs_crimeagainst," \
                                "x_coordinate,y_cordinate,zip_code,city,geocoded_column",
                                where=f"year1={year}", 
                                limit=base_limit, 
                                offset=base_offset)
        if not batch_result:
            break
        batches.append(pd.DataFrame.from_records(batch_result))
        base_offset+=base_limit

    # Edge Case Check: If no data found and batches remains empty.
    if batches:
        results_df = pd.concat(batches, ignore_index=True)

        output_path = Path(main_path / "data_raw/crime_data")
        output_path.mkdir(parents=True, exist_ok=True)

        results_df.to_csv(output_path  / f"dallas_crime_raw_{year}.csv", index=False)
    return results_df

def get_crimes_df(year, path_crime_data_raw, type):
    """
    Get crime of type V (violent) or P (property), return as dataframe
    """
    if type == 'V':
        filename = f"crime_violent_{year}.csv"
    elif type == 'P':
        filename = f"crime_property_{year}.csv"
    else:
        return None
    
    if Path(path_crime_data_raw / filename).exists():
        crime_df = pd.read_csv(path_crime_data_raw / filename)
    else:
        agency_city = pd.read_csv(main_path / "data_raw/tables/agency_city.csv")
        crime_df = pd.DataFrame(columns=["agency","month","offenses_per_100k","offenses","clearances","population"])

        for row in agency_city.itertuples():
            crime_v_agency_df = pull_crime_by_agency(row.agency_id,row.agency_name,year,type)
            if crime_df.empty:
                crime_df = crime_v_agency_df
            else:
                crime_df = pd.concat([crime_df, crime_v_agency_df], ignore_index=True)

        crime_df.to_csv(path_crime_data_raw / filename, index=False)
    return crime_df

# School Rating Pulling
def get_school_rating(year=None):
    """
    Get school rating from self database/csv file
    """
    school_data_path = Path(main_path / "data_raw/school_data")

    if 2017 < year < 2023: # uses xlsx files
        # This era has 'County' listed as well as rating through 'Overall Rating' and 'Overall Score'
        school_rating_df = pd.read_excel(school_data_path / f"{year}_school_raw.xlsx", usecols=['District','Campus','Region','County','School\nType','Overall\nRating','Overall\nScore'])
        school_rating_df = school_rating_df[school_rating_df['County'] == 'DALLAS']
        school_rating_df = school_rating_df.rename(columns={'School\nType' : 'SchoolType', 'Overall\nRating' : 'OverallRating', 'Overall\nScore' : 'OverallScore'})
        return school_rating_df
    elif 2022 < year:
        # County now has id, countyname in 'CNTYNAME', ratings through 'C_RATING' and 'CDALLS'
        school_rating_df = pd.read_csv(school_data_path / f"{year}_school_raw.csv", usecols=['CAMPNAME','DISTNAME','CNTYNAME','REGNNAME','C_RATING','CDALLS'])
        school_rating_df = school_rating_df[school_rating_df['CNTYNAME'] == 'DALLAS']
        return school_rating_df
    else:
        return None

    
