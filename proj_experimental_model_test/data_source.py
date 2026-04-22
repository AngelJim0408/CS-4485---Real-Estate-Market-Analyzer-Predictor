# Data Source Program
# - Pulls data from web sources and stores directly into the SQLite database
#   via the RealEstateDB class (database.py).
# - All get_* functions check the DB first; only fetch from the web if missing.

import os
import time
import requests
import pandas as pd
from io import StringIO, BytesIO
from datetime import date
from sodapy import Socrata
from pathlib import Path
from dotenv import load_dotenv

from database import RealEstateDB

load_dotenv()

ACS_LATEST_YEAR    = 2024
DALLAS_COUNTY_GEOID = 48113

FBI_API_KEY = os.getenv("FBI_API_KEY")

current_file_path = Path(__file__).resolve()
main_path         = Path(current_file_path.parent)

url_fbi = "https://api.usa.gov/crime/fbi/cde"

# Warn immediately if FBI key is missing
if not FBI_API_KEY:
    print(
        "\n[WARNING] FBI_API_KEY is not set."
        "\n  Crime data will be skipped until you add it."
        "\n  Add FBI_API_KEY=your_key to your .env file (same folder as main.py)."
        "\n  Get a key at: https://api.usa.gov/crime/fbi/cde\n"
    )

# ------------------------------------------------------------------
# Shared DB instance — created once when this module is imported.
# All get_*/pull_* functions use this same connection.
# ------------------------------------------------------------------
DB_PATH = main_path / "real_estate.db"
db = RealEstateDB(DB_PATH)
db.create_tables()   # safe no-op if tables already exist


# ------------------------------------------------------------------
# BASE WORKER FUNCTIONS
# ------------------------------------------------------------------
def get_zcta_county(year, county_id=48113):
    url   = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"
    cross = pd.read_csv(url, sep='|', dtype=str)
    return cross[cross["GEOID_COUNTY_20"] == f'{county_id}']["GEOID_ZCTA5_20"].dropna().unique()

def download_tea_school_directory(save_path):
    url = "https://tealprod.tea.state.tx.us/Tea.AskTed.Web/Forms/DownloadDefault.aspx"
    r   = requests.get(url)
    with open(save_path, "wb") as f:
        f.write(r.content)
    print(f"Saved: {save_path}")


# ------------------------------------------------------------------
# INTERNAL DB HELPERS  (thin wrappers around RealEstateDB.query)
# ------------------------------------------------------------------
def _has_rows(table: str, where: str = "") -> bool:
    """Returns True if the table exists and contains at least one matching row."""
    try:
        sql    = f"SELECT 1 FROM {table}"
        sql   += f" WHERE {where}" if where else ""
        sql   += " LIMIT 1"
        result = db.query(sql)
        return len(result) > 0
    except Exception:
        return False

def _read(table: str, where: str = "") -> pd.DataFrame:
    """Read all rows (optionally filtered) from a table."""
    sql  = f"SELECT * FROM {table}"
    sql += f" WHERE {where}" if where else ""
    return db.query(sql)

def _save(df: pd.DataFrame, table: str, if_exists: str="replace"):
    """Upsert a DataFrame into the database via RealEstateDB._upsert_df."""
    db._upsert_df(df, table, if_exists)


# ------------------------------------------------------------------
# LOOKUP / REFERENCE TABLES
# ------------------------------------------------------------------
def get_campus_zip_data(year=None):
    """
    Returns campus-to-ZIP mapping for Dallas County schools.
    Raw DB table: campus_zip  (campus_id, campus, zipcode, year)
    """
    table        = "campus_zip"
    where_clause = f"year = {year}" if year is not None else "year IS NULL"

    if _has_rows(table, where_clause):
        print(f"[DB] Loading campus_zip (year={year})")
        return _read(table, where_clause)

    if year is not None:
        curr_year = date.today().year
        if year in range(2018, curr_year):
            year        = max(2019, year)
            target_path = main_path / f"data_raw/school_data/school_district_full_crossroad/ArchivedSchoolAndDistrictSpring{year}.csv"
        else:
            print(f"File not found for year:{year}")
            return None
    else:
        target_path = main_path / "data_raw/school_data/school_directory.csv"

    if not Path(target_path).exists():
        download_tea_school_directory(target_path)

    df = pd.read_csv(target_path)
    df = df[df['County Name'] == 'DALLAS COUNTY'][['School Number', 'School Name', 'School Zip']]
    df['School Number'] = df['School Number'].str[1:]
    df.rename(columns={'School Number': 'campus_id', 'School Name': 'campus', 'School Zip': 'zipcode'}, inplace=True)
    df['year'] = year

    _save(df, table)
    return df

def get_lookup_table(filename):
    """Lookup tables are small static CSVs — always read from disk."""
    return pd.read_csv(main_path / f"data_raw/tables/{filename}")


# ------------------------------------------------------------------
# 1. PRICE MOMENTUM
# ------------------------------------------------------------------
def pull_zhvi_data():
    url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

def get_zhvi_data():
    """
    Raw DB table: zhvi  (zipcode, year, month, zhvi)
    Normalised long-format stored by data_normalize; here we return
    the wide-format Zillow frame that normalize expects as input.
    """
    table = "zhvi_raw"   # separate from the processed 'zhvi' table
    if _has_rows(table):
        print("[DB] Loading ZHVI raw from database")
        return _read(table)

    df = pull_zhvi_data()
    df = df[df["CountyName"] == "Dallas County"].copy()
    df = df[df["StateName"]  == "TX"]
    df.drop(columns=['RegionID','SizeRank','RegionType','StateName','State','City','Metro','CountyName'], inplace=True)
    df.rename(columns={'RegionName': 'zipcode'}, inplace=True)

    _save(df, table)
    return df


# ------------------------------------------------------------------
# 2. SUPPLY & DEMAND
# ------------------------------------------------------------------
"""
def pull_zillow_sales():
    url = "https://files.zillowstatic.com/research/public_csvs/sales_count_now/Metro_sales_count_now_uc_sfrcondo_month.csv"
    return pd.read_csv(StringIO(requests.get(url).text))

def pull_zillow_rent():
    url = "https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"
    return pd.read_csv(StringIO(requests.get(url).text))

def pull_zillow_listings():
    url = "https://files.zillowstatic.com/research/public_csvs/new_listings/Metro_new_listings_uc_sfrcondo_sm_month.csv"
    return pd.read_csv(StringIO(requests.get(url).text))

def pull_zillow_inv():
    url = "https://files.zillowstatic.com/research/public_csvs/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv"
    return pd.read_csv(StringIO(requests.get(url).text))
"""
def pull_redfin_zip(cols, dmap):
    """
    Downloads Redfin ZIP-level market tracker (~1 GB) to a temp file.
    Must use a temp file — too large to hold in memory all at once.
    Once processed and saved to DB, the temp file can be deleted.
    """
    import tempfile
    url = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz"
    temp_path = Path(tempfile.gettempdir()) / "redfin_market_tracker.tsv000.gz"

    print(f"[Redfin] Downloading ~1GB file to temp: {temp_path}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(temp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return pd.read_csv(temp_path, sep='\t', compression='gzip',
                       usecols=cols, dtype=dmap, chunksize=200_000, low_memory=True)

def _clean_zillow_supply(df: pd.DataFrame) -> pd.DataFrame:
    """Filter a raw Zillow supply DataFrame to Dallas County TX."""
    df = df[df["StateName"] == "TX"]
    if 'CountyName' in df.columns:
        df = df[df["CountyName"] == "Dallas County"].copy()
        df.drop(columns=['RegionID','SizeRank','RegionType','StateName','State','City','Metro','CountyName'], inplace=True)
        df.rename(columns={'RegionName': 'zipcode'}, inplace=True)
    else:
        df = df[df["RegionName"] == "Dallas, TX"].copy()
        df.drop(columns=['RegionID','SizeRank','RegionType','StateName'], inplace=True)
        df.rename(columns={'RegionName': 'msa'}, inplace=True)
    return df


def get_zillow_supply(type):
    """
    Raw DB tables: zillow_raw_sales_count | zillow_raw_rent |
                   zillow_raw_new_listings | zillow_raw_inventory
    Wide-format frames returned for data_normalize to process.
    types: sales_count | rent | new_listings | inventory
    """
    table = f"zillow_raw_{type}"
    if _has_rows(table):
        print(f"[DB] Loading {table} from database")
        return _read(table)

    match type:
        case 'sales_count': raw = pull_zillow_sales()
        case 'rent':        raw = pull_zillow_rent()
        case 'new_listings': raw = pull_zillow_listings()
        case 'inventory':   raw = pull_zillow_inv()
        case _:             return None

    df = _clean_zillow_supply(raw)
    _save(df, table)
    return df

def get_redfin(zipcodes):
    """
    Raw DB table: redfin_supply  (date, date_end, zipcode, sales_count, new_listings, inventory)
    """
    table = "redfin_supply"
    if _has_rows(table):
        print("[DB] Loading Redfin data from database")
        return _read(table)

    cols_to_use = ["PERIOD_BEGIN","PERIOD_END","REGION","REGION_TYPE",
                   "STATE_CODE","PROPERTY_TYPE","HOMES_SOLD","NEW_LISTINGS","INVENTORY"]
    dtype_map   = {"REGION":"string","STATE_CODE":"string","REGION_TYPE":"string",
                   "PROPERTY_TYPE":"string","HOMES_SOLD":"float32",
                   "NEW_LISTINGS":"float32","INVENTORY":"float32"}

    import tempfile
    temp_path = Path(tempfile.gettempdir()) / "redfin_market_tracker.tsv000.gz"
    reader = (
        pd.read_csv(temp_path, sep='\t', compression='gzip', usecols=cols_to_use,
                    dtype=dtype_map, chunksize=200_000, low_memory=True)
        if temp_path.exists()
        else pull_redfin_zip(cols_to_use, dtype_map)
    )

    dallas_zips = set(zipcodes['zipcode'].astype(str))
    chunks = []
    for chunk in reader:
        chunk = chunk[
            (chunk['STATE_CODE']   == 'TX') &
            (chunk['PROPERTY_TYPE']== 'All Residential') &
            (chunk['REGION_TYPE']  == 'zip code')
        ]
        chunk['REGION'] = chunk['REGION'].astype(str).str.extract(r'(\d{5})')[0]
        chunk = chunk[chunk['REGION'].isin(dallas_zips)]
        chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True)
    df.drop(columns=['STATE_CODE','PROPERTY_TYPE','REGION_TYPE'], inplace=True)
    df.rename(columns={'PERIOD_BEGIN':'date','PERIOD_END':'date_end','REGION':'zipcode',
                       'HOMES_SOLD':'sales_count','NEW_LISTINGS':'new_listings',
                       'INVENTORY':'inventory'}, inplace=True)

    _save(df, table)
    return df


# ------------------------------------------------------------------
# 3. ECONOMIC ENVIRONMENT
# ------------------------------------------------------------------
def pull_mortgage_rates():
    url = "https://www.freddiemac.com/pmms/docs/historicalweeklydata.xlsx"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_excel(BytesIO(response.content), skiprows=6)

def pull_unemployment(year_end, year_start):
    url  = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    rows = []
    for start in range(year_start, year_end + 1, 10):
        end     = min(start + 9, year_end)
        payload = {'seriesid': ['LAUCN481130000000003'], 'startyear': str(start), 'endyear': str(end)}
        data    = requests.post(url, json=payload).json()
        for item in data["Results"]["series"][0]["data"]:
            if item["period"].startswith("M"):
                rate = item["value"]
                rows.append({
                    "year":             int(item["year"]),
                    "month":            int(item["period"][1:]),
                    "unemployment_rate": float(rate) if rate != '-' else None
                })
    return pd.DataFrame(rows).sort_values(["year","month"]).reset_index(drop=True)

def pull_med_income(year):
    url      = f"https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E,B01003_001E&ucgid=pseudo(0400000US48$8600000)"
    response = requests.get(url)
    response.raise_for_status()
    data     = response.json()

    zcta_county = get_zcta_county(year, DALLAS_COUNTY_GEOID)
    df          = pd.DataFrame(data[1:], columns=data[0])
    df['NAME']  = df['NAME'].str.slice(start=6)
    df          = df[df['NAME'].isin(zcta_county)]
    df.rename(columns={'NAME':'ZCTA','B19013_001E':'median_income','B01003_001E':'total_population'}, inplace=True)
    df["ZCTA"]             = df["ZCTA"].astype(str)
    df["median_income"]    = pd.to_numeric(df["median_income"],    errors="coerce")
    df["total_population"] = pd.to_numeric(df["total_population"], errors="coerce")
    df.drop(columns=["ucgid"], inplace=True)
    df["year"] = year
    return df

def get_mortgage_rates():
    """
    Raw DB table: mortgage_rates_raw
    Wide weekly frame; data_normalize resamples it to monthly.
    """
    table = "mortgage_rates_raw"
    if _has_rows(table):
        print("[DB] Loading mortgage rates from database")
        return _read(table)

    df = pull_mortgage_rates()
    df = df.iloc[:-1]   # drop trailing disclaimer row
    df.columns = ['Week','US30yrFRM','30yrFeesPoints','US15yrFRM',
                  '15yrFeesPoints','5/1ARM','5/1ARM_feesPoints',
                  '5/1ARM_margin','30yrFRM/5/1ARM_spread']
    _save(df, table)
    return df

def get_unemployment(year, year_earliest):
    """
    Processed DB table: unemployment  (year, month, unemployment_rate)
    Already in normalised form — no separate proc step needed.
    """
    table = "unemployment"
    if _has_rows(table):
        print("[DB] Loading unemployment from database")
        return _read(table)

    df = pull_unemployment(year, year_earliest)
    _save(df, table)
    return df

def get_med_income(year):
    """
    Raw DB table: median_income_raw (partitioned by year)
    data_normalize.normalize_income() will clean it further.
    """
    year  = min(year, ACS_LATEST_YEAR)
    table = "median_income_raw"

    if _has_rows(table, f"year = {year}"):
        print(f"[DB] Loading median income raw (year={year})")
        return _read(table, f"year = {year}")

    df = pull_med_income(year)
    df = df[df['median_income'] != -666666666]
    _save(df, table, "append")
    return df


# ------------------------------------------------------------------
# 4. NEIGHBORHOOD QUALITY — SCHOOL RATINGS
# ------------------------------------------------------------------
def get_school_rating(year=None):
    """
    Raw DB table: school_ratings_raw (partitioned by year)
    NOTE: Texas did NOT publish ratings in 2020 or 2021.
    """
    # TEA ratings not published in 2020 or 2021
    if year == 2020 or year == 2021:
        return None

    table = "school_ratings_raw"
    if _has_rows(table, f"year = {year}"):
        print(f"[DB] Loading school ratings raw (year={year})")
        return _read(table, f"year = {year}")

    school_data_path = main_path / "data_raw/school_data"

    if year == 2018:
        df = pd.read_excel(
            school_data_path / f"{year}_school_raw.xlsx",
            usecols=['Campus\nNumber','District Name','Region Name','County Name','Overall\nScore'],
            dtype={"Campus\nNumber": str}
        )
        df = df[df['County Name'] == 'DALLAS'].dropna(subset=['Campus\nNumber','Overall\nScore'])
        df.rename(columns={'Campus\nNumber':'campus_id','District Name':'district',
                           'Region Name':'region','County Name':'county','Overall\nScore':'score'}, inplace=True)
        df['district'] = df['district'].str.replace('\n',' ')
        df['region']   = df['region'].str.slice(start=11)

    elif 2018 < year < 2023:
        skiprows = 2 if year == 2019 else 0
        df = pd.read_excel(
            school_data_path / f"{year}_school_raw.xlsx", skiprows=skiprows,
            usecols=['Campus\nNumber','District','Region','County','Overall\nScore'],
            dtype={"Campus\nNumber": str}
        )
        df = df[df['County'] == 'DALLAS'].dropna(subset=['Campus\nNumber','Overall\nScore'])
        df.rename(columns={'Campus\nNumber':'campus_id','District':'district',
                           'Region':'region','County':'county','Overall\nScore':'score'}, inplace=True)
        df['district'] = df['district'].str.replace('\n',' ')
        df['region']   = df['region'].str.slice(start=10)

    elif year > 2022:
        df = pd.read_csv(
            school_data_path / f"{year}_school_raw.csv",
            usecols=['CAMPUS','DISTNAME','CNTYNAME','REGNNAME','CDALLS'],
            dtype={"CAMPUS": str}
        )
        df = df[df['CNTYNAME'] == 'DALLAS'].dropna(subset=['CAMPUS','CDALLS'])
        df.rename(columns={'CAMPUS':'campus_id','DISTNAME':'district',
                           'CNTYNAME':'county','REGNNAME':'region','CDALLS':'score'}, inplace=True)
        df['region'] = df['region'].str.slice(start=10)
        df['score']  = df['score'].astype(int)
    else:
        return None

    df['year'] = year
    _save(df, table, "append")
    return df


# ------------------------------------------------------------------
# 5. SAFETY — CRIME DATA
# ------------------------------------------------------------------
def pull_fbi_agencies(state="TX", county_filter="DALLAS"):
    """
    DB table: fbi_agencies
    Pulls and caches agency list from FBI CDE API.
    """
    table = "fbi_agencies"
    if _has_rows(table):
        print("[DB] Loading FBI agencies from database")
        return _read(table)

    url_agencies = f"{url_fbi}/agency/byStateAbbr/{state}?API_KEY={FBI_API_KEY}"
    response     = requests.get(url_agencies)
    response.raise_for_status()
    data = response.json()

    agencies = []
    for county_key, agency_list in data.items():
        if county_filter in county_key:
            agencies.extend(agency_list)

    if agencies:
        df = pd.json_normalize(agencies)
        _save(df, table)
        return df
    return pd.DataFrame()

def pull_crime_by_agency(agency_ori, agency_name, year, offense="V"):
    """Pulls monthly crime stats for one agency from FBI CDE API."""
    if not FBI_API_KEY:
        return pd.DataFrame()

    url_summary = f"{url_fbi}/summarized/agency/{agency_ori}/{offense}?"
    params      = {'from': f'01-{year}', 'to': f'12-{year}', 'API_KEY': f'{FBI_API_KEY}'}

    try:
        time.sleep(0.3)  # small delay to avoid overwhelming the FBI API
        response = requests.get(url_summary, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[FBI API] Skipping {agency_ori} {year}: {e}")
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    try:
        rows              = []
        offense_per_100k  = data["offenses"]["rates"][f"{agency_name} Offenses"]
        offenses          = data["offenses"]["actuals"][f"{agency_name} Offenses"]
        clearances        = data["offenses"]["actuals"][f"{agency_name} Clearances"]
        population        = data["populations"]["population"][f"{agency_name}"]

        for month in offenses:
            rows.append({
                "agency":           agency_ori,
                "month":            month,
                "offenses_per_100k": offense_per_100k[month],
                "offenses":         offenses[month],
                "clearances":       clearances[month],
                "population":       population[month]
            })
        return pd.DataFrame(rows)
    except (KeyError, TypeError) as e:
        print(f"[FBI API] Could not parse response for {agency_ori} {year}: {e}")
        return pd.DataFrame()

def pull_dallas_crime(year=None):
    """
    DB table: dallas_crime (partitioned by year1)
    Pulls Dallas PD police incidents from Dallas Open Data (Socrata).
    """
    if year is None:
        year = date.today().year - 1

    table = "dallas_crime"
    if _has_rows(table, f"year1 = {year}"):
        print(f"[DB] Dallas crime ({year}) already in database, skipping.")
        return _read(table, f"year1 = {year}")

    client      = Socrata("www.dallasopendata.com", None)
    base_limit  = 1000
    base_offset = 0
    batches     = []

    while True:
        batch_result = client.get(
            "qv6i-rri7",
            select="date1,year1,nibrs_crime,nibrs_crime_category,nibrs_crimeagainst,"
                   "x_coordinate,y_cordinate,zip_code,city,geocoded_column",
            where=f"year1={year}",
            limit=base_limit,
            offset=base_offset
        )
        if not batch_result:
            break
        batches.append(pd.DataFrame.from_records(batch_result))
        base_offset += base_limit

    if batches:
        df = pd.concat(batches, ignore_index=True)
        _save(df, table)
        return df
    return pd.DataFrame()

def get_crimes_df(year, type):
    """
    DB tables: crime_violent | crime_property  (partitioned by year)
    Columns: agency, month, offenses_per_100k, offenses, clearances, population, year
    """
    if type == 'V':
        table = "crime_violent_raw"
    elif type == 'P':
        table = "crime_property_raw"
    else:
        return None

    if _has_rows(table, f"year = {year}"):
        print(f"[DB] Loading {table} (year={year})")
        return _read(table, f"year = {year}")

    # agency_city is a static lookup — read from disk (small file, never changes)
    agency_city = get_lookup_table("agency_city.csv")
    frames      = []

    for row in agency_city.itertuples():
        agency_df = pull_crime_by_agency(row.agency, row.agency_name, year, type)
        if not agency_df.empty:
            frames.append(agency_df)

    if frames:
        crime_df         = pd.concat(frames, ignore_index=True)
        crime_df = crime_df.drop(columns='population')
        crime_df['year'] = year
        _save(crime_df, table, "append")
        return crime_df

    return pd.DataFrame(columns=["agency","month","offenses_per_100k","offenses","clearances","population","year"])