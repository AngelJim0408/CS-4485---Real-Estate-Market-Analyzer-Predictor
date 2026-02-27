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

def pull_fbi_agencies(state="TX"):
    url_agencies = f"{url_fbi}/agency/byStateAbbr/{state}?API_KEY={FBI_API_KEY}"

    response = requests.get(url_agencies)
    response.raise_for_status() # check for success
    data = response.json()

    results_df = pd.json_normalize(data)
    results_df = pd.DataFrame(results_df)

    # Save to csv so we don't need to call api
    # create path if does not exist
    output_path = Path(main_path / "data_raw/crime_data/fbi_data")
    output_path.mkdir(parents=True, exist_ok=True)

    results_df.to_csv(output_path  / f"fbi_agencies.csv", index=False, lineterminator="\n")

def pull_fbi_crime(year=None,city=None,state="TX"):
    """
    Default for when cities don't provide own data. Yearly crime incident reports (NIBRS)
    https://cde-prd-data.s3.us-gov-east-1.amazonaws.com/nibrs/incident/2024/TX-2024.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAQC732REK36MXHWF2%2F20260227%2Fus-gov-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260227T173535Z&X-Amz-Expires=900&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEFIaDXVzLWdvdi1lYXN0LTEiRzBFAiADxNkB9kIJkGZ12DK3hWwMF5gnWoSprElf8FzNYwwpYAIhAJvMaOAoQnmWcjeYfBp0nh8734LnjGkDb349cKRLDwRgKooDCH8QABoMMDA2NDMzNzA0MjEzIgzLkDKZgnGc%2B8kfQ3Mq5wLIkwNY%2BzUW5br%2F06ANgjpOgjZVCGEyTTQW5iVgaMjlhR9XRV9hpd2BZrSvq2Gsjz4U4ieGXm36kzrk8lLR8SpCXnZOsilOie2A9cbBZS0YW%2FcLoJNkIIIhOMLOy3Fzo%2Fc8tKpHduZu%2B3ROvdHzNKdBRdS%2FOBOlVfX2c8mrLqUCNtxFUWQiig6ULBwK%2BQ%2BO3LQRcJ8l3dXguxJiHRKPoxSwnwO1nur4C4bMlvaPcf%2FlR%2BA7McL9g3X%2BWVJiJlrOhovRV7gkNpXLiq7jMt9Cgkx2n154QSqqdD5HSkEg9vQy%2BNtrpi%2BleJYFXRYaqu9hw8yfGeQPdLzqN%2FUFjEEQSkATcYiQG1GtHxw7pvtrkRNzXGl4ck8zn5mZkO8HPvjn7MdlWv4cPCGcUtrhIyjk0U8Eh6ZjgVG7iS3XdBC96kozeRV1X27Gxn93ULv6n7t5Tz0x%2FRxo2gpVe9JDUJ1qYeTn06TJbuiaejDYmIfNBjqdAccbfAwgOrR%2FZu23S14fPVFyuUjD2688E9GMFgEjzKKQW9oIynsVVcfcALUQ1wtoZUC3RF4%2BykFQ%2Fl9w6k49z%2FSs6D9IUvzvdDz3L%2FR19ArjMXXnSoj41lZvRhIum%2Bb39zz9myNIifkE58%2BcDF6TUN4ulkw1xDEm7bU5QvcJEmzqcnKXDNbXKft3QvI58eWGwr5DtWWSvjQXlqCoVeI%3D&X-Amz-Signature=b190f8e31c3de520561adf3e0e8337885b0b817d8bafb36efa888173ec519ead&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject
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

        # create path if does not exist
        output_path = Path(main_path / "data_raw/crime_data")
        output_path.mkdir(parents=True, exist_ok=True)

        results_df.to_csv(output_path  / f"dallas_crime_raw_{year}.csv", index=False)
    return results_df
    
