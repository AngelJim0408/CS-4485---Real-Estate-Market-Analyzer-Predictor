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

def pull_crime_by_agency(agency_ori, offense="V"):
    url_summary = f"{url_fbi}/summarized/agency/{agency_ori}/{offense}?API_KEY={FBI_API_KEY}"

    response = requests.get(url_summary)
    response.raise_for_status()
    data = response.json()

    results_df = pd.json_normalize(data)

    if not results_df:
        results_df = pd.DataFrame()

    return results_df

def pull_fbi_crime(year=None,city=None,state="TX"):
    """
    Default for when cities don't provide own data. Yearly crime incident reports (NIBRS)
    - pulls fbi crime data by agency
    """
    url_violent = f"{url_fbi}/summarized/{state}/V"
    url_property = f"{url_fbi}/summarized/{state}/P"
    response = requests.get(url_violent)

    

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
    
