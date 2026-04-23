import pandas as pd
import logging as log
import datetime as dt

TABLE_FREQUENCY = {
    "mortgage_rates": "weekly",

    "zhvi": "monthly",
    "redfin_supply": "monthly",
    "unemployment": "monthly",
    "crime_violent": "monthly",
    "crime_property": "monthly",

    "school_ratings": "yearly",
    "median_income": "yearly",
}

FREQUENCY_DAYS = {
    "weekly": 7,
    "monthly": 30,
    "yearly": 365,
}
""
# ----------------------------------------------------------
#         Raw Data Update Monthly and Yearly
# ----------------------------------------------------------
def _get_time():
    return dt.datetime.now().isoformat()

def can_update(db, table: str):
    """
    Return True is table is able to be updated, else return false.
    """
    last_upd = db.get_last_update(table) # last_upd string (ISO format time)
    if not last_upd:
        return True # no data yet, so can update
    
    last_upd_dt = dt.datetime.fromisoformat(last_upd)
    curr_dt = dt.datetime.now()

    freq = TABLE_FREQUENCY[table]
    threshold = FREQUENCY_DAYS[freq]

    return (curr_dt - last_upd_dt).days >= threshold

# ---------------------------------------------------------------------------
# Per-table incremental updaters
# ---------------------------------------------------------------------------

def _update_mortgage_rates(db, ds, dn):
    """
    Fetches the latest Freddie Mac weekly data, keeps only rows newer than
    what's already in the DB, normalises, and appends.
    """
    max_year, max_month = db.get_max_period("mortgage_rates")

    raw  = ds.pull_mortgage_rates()
    raw  = raw.iloc[:-1]   # drop trailing disclaimer row
    raw.columns = [
        "Week", "US30yrFRM", "30yrFeesPoints", "US15yrFRM",
        "15yrFeesPoints", "5/1ARM", "5/1ARM_feesPoints",
        "5/1ARM_margin", "30yrFRM/5/1ARM_spread",
    ]

    proc = dn.normalize_mortgage(raw)   #(year, month, mortgage_rate)

    if max_year is not None:
        proc = proc[
            (proc["year"] > max_year) |
            ((proc["year"] == max_year) & (proc["month"] > max_month))
        ]

    db.append_df(proc, "mortgage_rates")


def _update_zhvi(db, ds, dn):
    """
    Pulls the latest Zillow ZHVI CSV, normalises to long format,
    and appends only months not yet in the DB.
    """
    max_year, max_month = db.get_max_period("zhvi")

    raw  = ds.pull_zhvi_data()
    raw  = raw[raw["CountyName"] == "Dallas County"] & [raw["StateName"] == "TX"].copy()
    raw.drop(
        columns=["RegionID", "SizeRank", "RegionType", "StateName",
                 "State", "City", "Metro", "CountyName"],
        inplace=True,
    )
    raw.rename(columns={"RegionName": "zipcode"}, inplace=True)

    proc = dn.normalize_zillow_data(raw, "zhvi")   #(zipcode, year, month, zhvi)

    if max_year is not None:
        proc = proc[
            (proc["year"] > max_year) |
            ((proc["year"] == max_year) & (proc["month"] > max_month))
        ]

    db.append_df(proc, "zhvi")


def _update_redfin(db, ds, dn, zipcodes):
    """
    Re-downloads the Redfin tracker (large file) but only keeps rows
    after the latest date already stored, then appends.
    """
    max_year, max_month = db.get_max_period("redfin_supply")

    # Must call pull_* directly: get_redfin() would return the DB cache
    cols_to_use = ["PERIOD_BEGIN","PERIOD_END","REGION","REGION_TYPE",
                   "STATE_CODE","PROPERTY_TYPE","HOMES_SOLD","NEW_LISTINGS","INVENTORY"]
    dtype_map   = {"REGION":"string","STATE_CODE":"string","REGION_TYPE":"string",
                   "PROPERTY_TYPE":"string","HOMES_SOLD":"float32",
                   "NEW_LISTINGS":"float32","INVENTORY":"float32"}
    dallas_zips = set(zipcodes["zipcode"].astype(str))
    chunks, cutoff = [], None

    if max_year and max_month:
        cutoff = dt.date(max_year, max_month, 1)

    for chunk in ds.pull_redfin_zip(cols_to_use, dtype_map):
        chunk = chunk[
            (chunk["STATE_CODE"]    == "TX") &
            (chunk["PROPERTY_TYPE"] == "All Residential") &
            (chunk["REGION_TYPE"]   == "zip code")
        ]
        chunk["REGION"] = chunk["REGION"].astype(str).str.extract(r"(\d{5})")[0]
        chunk = chunk[chunk["REGION"].isin(dallas_zips)]

        if cutoff:
            # Filter to only rows newer than what's already stored
            chunk = chunk[pd.to_datetime(chunk["PERIOD_BEGIN"]).dt.date > cutoff]

        if not chunk.empty:
            chunks.append(chunk)

    if not chunks:
        print("[UPDATE] No new Redfin rows found.")
        return

    df = pd.concat(chunks, ignore_index=True)
    df.drop(columns=["STATE_CODE","PROPERTY_TYPE","REGION_TYPE"], inplace=True)
    df.rename(columns={
        "PERIOD_BEGIN": "date", "PERIOD_END": "date_end", "REGION": "zipcode",
        "HOMES_SOLD": "sales_count", "NEW_LISTINGS": "new_listings", "INVENTORY": "inventory"
    }, inplace=True)

    proc = dn.normalize_redfin_data(df)
    db.append_df(proc, "redfin_supply")

def _update_unemployment(db, ds):
    """
    Pulls BLS unemployment only for years not yet fully stored.
    The BLS API accepts year ranges so this is a cheap targeted call.
    """
    max_year, _ = db.get_max_period("unemployment")
    curr_year   = dt.date.today().year

    # Start one year before max to catch any late-arriving monthly revisions
    year_start  = (max_year - 1) if max_year else 2010

    new_df = ds.pull_unemployment(curr_year, year_start)

    if max_year is not None:
        # Drop rows we already have (except the overlap year for revisions)
        new_df = new_df[new_df["year"] >= max_year]

    db.append_df(new_df, "unemployment")


def _update_crime(db, ds, dn, crime_type: str):
    table             = "crime_violent" if crime_type == "V" else "crime_property"
    max_year, max_month = db.get_max_period(table)   # now using month too
    curr_year         = dt.date.today().year

    agency_city     = ds.get_lookup_table("agency_city.csv")
    zipcode_city    = ds.get_lookup_table("zipcode_city.csv")
    agency_zipcodes = (
        pd.merge(zipcode_city, agency_city, how="left", on="city")[["zipcode", "agency"]]
    )

    frames = []
    for year in range((max_year) if max_year else 2015, curr_year):
        for row in agency_city.itertuples():
            agency_df = ds.pull_crime_by_agency(row.agency, row.agency_name, year, crime_type)
            if agency_df.empty:
                continue

            # For the latest stored year, drop months already in the DB
            if max_year and max_month and year == max_year:
                agency_df["_month_num"] = pd.to_datetime(agency_df["month"], format="%m-%Y").dt.month
                agency_df = agency_df[agency_df["_month_num"] > max_month]
                agency_df.drop(columns=["_month_num"], inplace=True)

            if not agency_df.empty:
                agency_df["year"] = year
                frames.append(agency_df)

    if not frames:
        print(f"[UPDATE] No new {table} rows found.")
        return

    combined   = pd.concat(frames, ignore_index=True)
    normalised = dn.normalize_crime(combined, agency_zipcodes)
    normalised = normalised[
        ["zipcode", "agency", "year", "month",
         "offenses_per_100k", "offenses", "clearances"]
    ]
    db.append_df(normalised, table)


def _update_school_ratings(db, ds, dn):
    """
    Loads TEA ratings for any year not yet in the DB.
    (2020 and 2021 are skipped — Texas didn't publish ratings those years.)
    """
    max_year, _ = db.get_max_period("school_ratings", month_col=None)
    curr_year   = dt.date.today().year

    frames = []
    for year in range((max_year + 1) if max_year else 2018, curr_year):
        if year in (2020, 2021):
            continue
        rating_df = ds.get_school_rating(year)
        if rating_df is None or rating_df.empty:
            continue
        archive = ds.get_campus_zip_data(max(2019, year))
        if archive is not None:
            rating_df = dn.normalize_school(rating_df, archive)
            frames.append(rating_df)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        combined = combined[["zipcode", "campus_id", "year", "score"]]
        db.append_df(combined, "school_ratings")


def _update_median_income(db, ds, dn):
    """
    Fetches ACS median income for any year not yet stored.
    Caps at ACS_LATEST_YEAR defined in data_source.
    """
    max_year, _ = db.get_max_period("median_income", month_col=None)
    curr_year   = dt.date.today().year
    acs_cap     = ds.ACS_LATEST_YEAR

    frames = []
    for year in range((max_year + 1) if max_year else 2018, min(curr_year, acs_cap + 1)):
        income_df = ds.pull_med_income(year)
        if income_df is not None and not income_df.empty:
            income_df = income_df[income_df["median_income"] != -666666666]
            frames.append(dn.normalize_income(income_df))

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        combined = combined[["zipcode", "year", "median_income", "total_population"]]
        db.append_df(combined, "median_income")


# ---------------------------------------------------------------------------
# Update Runner
# ---------------------------------------------------------------------------

# Maps each logical table name to its updater function + extra kwargs
_UPDATERS = {
    "mortgage_rates":  lambda db, ds, dn, dc: _update_mortgage_rates(db, ds, dn),
    "zhvi":            lambda db, ds, dn, dc: _update_zhvi(db, ds, dn),
    "redfin_supply":   lambda db, ds, dn, dc: _update_redfin(db, ds, dn, ds.get_lookup_table("zipcodes.csv")),
    "unemployment":    lambda db, ds, dn, dc: _update_unemployment(db, ds),
    "crime_violent":   lambda db, ds, dn, dc: _update_crime(db, ds, dn, "V"),
    "crime_property":  lambda db, ds, dn, dc: _update_crime(db, ds, dn, "P"),
    "school_ratings":  lambda db, ds, dn, dc: _update_school_ratings(db, ds, dn),
    "median_income":   lambda db, ds, dn, dc: _update_median_income(db, ds, dn),
}


def run_update(db, data_class, table: str):
    """
    Runs an incremental update for a single table.
    data_class exposes .ds (data_source) and .dn (data_normalize).
    """
    ds = data_class.ds
    dn = data_class.dn

    try:
        db.update_running(_get_time(), table)
        _UPDATERS[table](db, ds, dn, data_class)
        db.update_status(_get_time(), table)
        print(f"[UPDATE] {table} complete")
    except Exception as e:
        db.update_fail(_get_time(), str(e), table)
        log.exception(f"[ERROR] {table}: {e}")


def update_all(data_class):
    """Check every table and run incremental updates for stale ones."""
    print("Starting data update...")
    db = data_class.db
    for table in TABLE_FREQUENCY:
        if can_update(db, table):
            print(f"Updating {table}...")
            run_update(db, data_class, table)
        else:
            db.update_skip(_get_time(), table)
