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

ACS_LATEST_YEAR = 2024
DALLAS_COUNTY_GEOID = 48113

FBI_API_KEY = os.getenv("FBI_API_KEY")

current_file_path = Path(__file__).resolve()
main_path = Path(current_file_path.parent)

url_fbi = "https://api.usa.gov/crime/fbi/cde"

# Base Worker functs/script
def writeWebToLocal(url, target_path):
    target_path.parent.mkdir(parents=True, exist_ok=True)

    req = requests.get(url)
    with open(target_path, "wb") as file:
        file.write(req.content)

def get_zcta_county(year, county_id=48113):
    url = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"

    cross = pd.read_csv(url,sep='|',dtype=str)
    county_zctas = cross[
        cross["GEOID_COUNTY_20"] == f'{county_id}' # ex 48113 = 48: TX, 113: Dallas County
    ]["GEOID_ZCTA5_20"].dropna().unique()
    

    return county_zctas

def download_tea_school_directory(save_path):
    """
    Downloads the TEA school and district data file for a given school year.
    """
    # AskTED download endpoint — county filter 057 = Dallas County
    url = (
        "https://tealprod.tea.state.tx.us/Tea.AskTed.Web/Forms/DownloadDefault.aspx"
    )
    r = requests.get(url)
    with open(save_path, "wb") as f:
        f.write(r.content)
    print(f"Saved: {save_path}")

def get_campus_zip_data(year=None):
    if year is not None:
        curr_year = date.today().year
        if year in range(2018, curr_year - 1):
            year = max(2019, year)
            target_path = main_path / f"data_raw/school_data/ArchivedSchoolAndDistrictSpring{year}.csv"
        else:
            print(f"File not found for year:{year}")
            return None
    else:
        target_path = main_path / "data_raw/school_data/school_directory.csv"

    if Path(target_path).exists():
        campus_zip_df = pd.read_csv(target_path)
    else:
        download_tea_school_directory(target_path)
        campus_zip_df = pd.read_csv(target_path)

    campus_zip_df = campus_zip_df[campus_zip_df['County Name'] == 'DALLAS COUNTY']
    campus_zip_df = campus_zip_df[['School Number','School Name','School Zip']]
    campus_zip_df['School Number'] = campus_zip_df['School Number'].str[1:]

    campus_zip_df.rename(columns={'School Number' : 'campus_id','School Name' : 'campus', 'School Zip' : 'zipcode'}, inplace=True)
    
    return campus_zip_df

def get_lookup_table(filename):
    target_path = main_path / f"data_raw/tables/{filename}"

    look_up_df = pd.read_csv(target_path)
    return look_up_df
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
    dallas_zhvi_df = zhvi_df[zhvi_df["CountyName"] == "Dallas County"].copy()

    dallas_zhvi_df.drop(columns=['RegionID','SizeRank','RegionType','StateName','State','City','Metro','CountyName'], inplace=True)
    dallas_zhvi_df.rename(columns={'RegionName' : 'zipcode'}, inplace=True)
    
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
    if 'CountyName' in zillow_supplydemand_df.columns:
        zillow_supplydemand_df = zillow_supplydemand_df[zillow_supplydemand_df["CountyName"] == "Dallas County"].copy()

        zillow_supplydemand_df.drop(columns=['RegionID','SizeRank','RegionType','StateName','State','City','Metro','CountyName'], inplace=True)
        zillow_supplydemand_df.rename(columns={'RegionName' : 'zipcode'}, inplace=True)
    else: # Doesn't use county, use msa (zillow data either in county zips or msa (Dallas-Fort Worth-Arlington))
        zillow_supplydemand_df = zillow_supplydemand_df[zillow_supplydemand_df["RegionName"] == "Dallas, TX"].copy()
        #RegionID,SizeRank,RegionName,RegionType,StateName, 
        zillow_supplydemand_df.drop(columns=['RegionID','SizeRank','RegionType','StateName'], inplace=True)
        zillow_supplydemand_df.rename(columns={'RegionName' : 'msa'}, inplace=True) # msa = Metropolitan Statistical Area

    return zillow_supplydemand_df

# 3. Economice Environment
def pull_mortgage_rates():
    target_path = main_path / "data_raw/economic_env/mortgage_rates_weekly.xlsx"
    url = "https://www.freddiemac.com/pmms/docs/historicalweeklydata.xlsx"
    writeWebToLocal(url, target_path)

    return pd.read_excel(target_path)

def pull_unemployment(year_end, year_start):
    target_path = main_path / "data_raw/economic_env/unemployment_rates.csv"
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    payload = {
        'seriesid' : ['LAUCN481130000000003'],
        'startyear' : f'{year_start}',
        'endyear' : f'{year_end}',
    }

    # TODO: Finish getting data, turn to csv file, return df
    req = requests.post(url, json=payload)
    data = req.json()

    rows = []

    for item in data["Results"]["series"][0]["data"]:
        if item["period"].startswith("M"):
            rate = item["value"]

            rows.append({
                "year": item["year"],
                "month": item["period"][1:],
                "unemployment_rate": float(rate) if rate != '-' else None
            })

    results_df = pd.DataFrame(rows)
    results_df.to_csv(target_path, index=False, lineterminator="\n")

    return results_df

def pull_med_income(year):
    target_path = main_path / f"data_raw/economic_env/median_household_income/income_population_{year}.csv"
    #url = f"https://api.census.gov/data/{year}/acs/acs5?get=group(B19013_001E)&ucgid=pseudo(0400000US48$8600000)"
    url = f"https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E,B01003_001E&ucgid=pseudo(0400000US48$8600000)"

    response = requests.get(url)
    response.raise_for_status() # check for success
    data = response.json()

    zcta_county = get_zcta_county(year, DALLAS_COUNTY_GEOID)

    results_df = pd.DataFrame(data[1:], columns=data[0])
    results_df['NAME'] = results_df['NAME'].str.slice(start=6)
    results_df = results_df[results_df['NAME'].isin(zcta_county)]
    results_df = results_df.rename(columns={'NAME' : 'ZCTA', 'B19013_001E' : 'median_income', 'B01003_001E' : 'total_population'})

    results_df["ZCTA"] = results_df["ZCTA"].astype(str)
    results_df["median_income"] = pd.to_numeric(results_df["median_income"], errors="coerce")
    results_df["total_population"] = pd.to_numeric(results_df["total_population"], errors="coerce")

    results_df = results_df.drop(columns=["ucgid"])

    results_df.to_csv(target_path, index=False, lineterminator="\n")

    return results_df

def get_mortgage_rates():
    target_path = main_path / "data_raw/economic_env/mortgage_rates_weekly.xlsx"

    if Path(target_path).exists():
        mortgage_df = pd.read_excel(target_path, skiprows=6)
    else:
        mortgage_df = pull_mortgage_rates()

    mortgage_df = mortgage_df.iloc[:-1] # Remove last row (its a disclaimer sentence)
    mortgage_df.columns = ['Week','US30yrFRM','30yerFeesPoints','US15yrFRM','15yrFeesPoints','5/1ARM','5/1ARM_feesPoints','5/1ARM_margin','30yrFRM/5/1ARM_spread']
    return mortgage_df

def get_unemployment(year):
    target_path = main_path / "data_raw/economic_env/unemployment_rates.csv"

    if Path(target_path).exists():
        unemployment_df = pd.read_csv(target_path)
    else:
        unemployment_df = pull_unemployment(year, year - 10) # range: 10 years ago - current year

    return unemployment_df

def get_med_income(year):
    year = min(year, ACS_LATEST_YEAR)
    target_path = main_path / f"data_raw/economic_env/median_household_income/income_population_{year}.csv"
    if Path(target_path).exists():
        med_income_df = pd.read_csv(target_path)
    else:
        med_income_df = pull_med_income(year)

    med_income_df = med_income_df[med_income_df['median_income'] != -666666666]
    return med_income_df


# 4. Neighborhood Quality
# School Rating Pulling
def get_school_rating(year=None):
    """
    Get school rating from self database/csv file
    """

    ## IMPORTANT: STATE OF TEXAS DID NOT PUBLISH RATINGS IN 2020 and 2021
    if year == 2020 or year == 2021:
        return None
    
    school_data_path = Path(main_path / "data_raw/school_data")

    if year == 2018:
        school_rating_df = pd.read_excel(school_data_path / f"{year}_school_raw.xlsx", usecols=['Campus\nNumber','District Name','Region Name','County Name','Overall\nScore'], dtype={"Campus\nNumber": str})
        school_rating_df = school_rating_df[school_rating_df['County Name'] == 'DALLAS']
        school_rating_df = school_rating_df.dropna(subset=['Campus\nNumber'])
        school_rating_df = school_rating_df.dropna(subset=['Overall\nScore'])

        school_rating_df.rename(columns={'Campus\nNumber' : 'campus_id','District Name' : 'district','Region Name' : 'region',
                                         'County Name': 'county','Overall\nScore' : 'score'}, inplace=True)
        
        school_rating_df['district'] = school_rating_df['district'].str.replace('\n',' ')
        school_rating_df['region'] = school_rating_df['region'].str.slice(start=11)
        
        return school_rating_df
    elif 2018 < year < 2023: # uses xlsx files
        skiprows = 0
        if year == 2019:
            skiprows = 2
        # This era has 'County' listed as well as rating through 'Overall Rating' and 'Overall Score'
        school_rating_df = pd.read_excel(school_data_path / f"{year}_school_raw.xlsx", skiprows=skiprows, usecols=['Campus\nNumber','District','Region','County','Overall\nScore'], dtype={"Campus\nNumber": str})
        school_rating_df = school_rating_df[school_rating_df['County'] == 'DALLAS']
        school_rating_df = school_rating_df.dropna(subset=['Campus\nNumber'])
        school_rating_df = school_rating_df.dropna(subset=['Overall\nScore'])
        
        school_rating_df.rename(columns={'Campus\nNumber' : 'campus_id','District' : 'district','Region': 'region','County' : 'county','Overall\nScore' : 'score'}, inplace=True)

        school_rating_df['district'] = school_rating_df['district'].str.replace('\n',' ')

        school_rating_df['region'] = school_rating_df['region'].str.slice(start=10)

        return school_rating_df
    
    elif 2022 < year:
        # County now has id, countyname in 'CNTYNAME', ratings through 'C_RATING' and 'CDALLS'
        school_rating_df = pd.read_csv(school_data_path / f"{year}_school_raw.csv", usecols=['CAMPUS','DISTNAME','CNTYNAME','REGNNAME','CDALLS'], dtype={"CAMPUS": str})
        school_rating_df = school_rating_df[school_rating_df['CNTYNAME'] == 'DALLAS']
        school_rating_df = school_rating_df.dropna(subset=['CAMPUS'])
        school_rating_df = school_rating_df.dropna(subset=['CDALLS'])

        school_rating_df.rename(columns={'CAMPUS' : 'campus_id','DISTNAME' : 'district','CNTYNAME' : 'county','REGNNAME' : 'region','CDALLS' : 'score'}, inplace=True)
        
        school_rating_df['region'] = school_rating_df['region'].str.slice(start=10)
        school_rating_df['score'] = school_rating_df['score'].astype(int)

        return school_rating_df
    else:
        return None
    
# 5. Safety
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

def get_crimes_df(year, type):
    """
    Get crime of type V (violent) or P (property), return as dataframe
    """
    if type == 'V':
        filepath = f"data_raw/crime_data/violent/crime_violent_{year}.csv"
    elif type == 'P':
        filepath = f"data_raw/crime_data/property/crime_property_{year}.csv"
    else:
        return None

    if Path(main_path / filepath).exists():
        crime_df = pd.read_csv(main_path / filepath)
    else:
        agency_city = pd.read_csv(main_path / "data_raw/tables/agency_city.csv")
        crime_df = pd.DataFrame(columns=["agency","month","offenses_per_100k","offenses","clearances","population"])

        for row in agency_city.itertuples():
            crime_v_agency_df = pull_crime_by_agency(row.agency_id,row.agency_name,year,type)
            if crime_df.empty:
                crime_df = crime_v_agency_df
            else:
                crime_df = pd.concat([crime_df, crime_v_agency_df], ignore_index=True)

        crime_df.to_csv(main_path / filepath, index=False)
    return crime_df
