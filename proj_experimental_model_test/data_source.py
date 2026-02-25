# Data Source Program
# - Provide functionality to pull sources dynamically from web
# - Access and save raw data

## Preparing Crime Data
# www.dallasopendata.com
# - Police Incidents (Dallas PD): Dataset ID (Socrata): qv6i-rri7
# pip install sodapy

import pandas as pd
from datetime import date
from sodapy import Socrata
from pathlib import Path

main_path = Path("proj_experimental_model_test")

def pull_dallas_crime(year=None):
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
                                "nibrs_crime,nibrs_crime_category,nibrs_crimeagainst,nibrs_code," \
                                "x_coordinate,y_cordinate,zip_code,geocoded_column",
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

        # create path if does not exist
        output_path = Path(main_path / "data_raw/crime_data")
        output_path.mkdir(parents=True, exist_ok=True)

        results_df.to_csv(output_path  / f"dallas_crime_raw_{year}.csv", index=False)
    return results_df
    
