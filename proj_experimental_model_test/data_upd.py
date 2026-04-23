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
"""
----------------------------------------------------------
        Raw Data Update Monthly and Yearly
----------------------------------------------------------

"""
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

def run_update(db, data_class, table: str):
    print

def check_update_status(db, timeframe, data_class, table: str):
    print()

def update_all(db, data_class):
    print("Starting data update...")

    for table in TABLE_FREQUENCY:
        if can_update(db, table):
            print(f"Updating {table}...")
            run_update(db, data_class, table)
        else:
            print(f"Skipping {table} (up-to-date)")