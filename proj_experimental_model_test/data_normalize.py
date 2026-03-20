import pandas as pd

def flatten_dataframes(data_dict: dict) -> pd.DataFrame:
    """
    Combines dataframe in a dictionary where the keys are years
    Returns: full combined dataframe with year as new column.
    """
    new_frames = []
    for year, dataframe in data_dict.items():
        if dataframe is not None:
            dataframe['year'] = year
            new_frames.append(dataframe)

    return pd.concat(new_frames, ignore_index=True)

def change_table_columns(dataframe):
    # get the dataframe
    # get columns corresponding to date,
    # create new columns for month and year, and value

    # assign ordingly
    # return the new dataframe with modified columns
    return dataframe

def build_merged_df(
    zhvi: pd.DataFrame,             # (date, zipcode, zhvi)
    sales: pd.DataFrame,            # (date, zipcode, sales_count)
    rent: pd.DataFrame,             # (date, zipcode, rent)
    listings: pd.DataFrame,         # (date, zipcode, listings)
    inventory: pd.DataFrame,        # (date, zipcode, inventory)
    mortgage: pd.DataFrame,         # (date, mortgage_rate)
    unemployment: pd.DataFrame,     # (date, unemployment_rate)
    income: pd.DataFrame,           # (date, zipcode, median_income)  — already ffilled to monthly
    school: pd.DataFrame,           # (date, zipcode, school_rating)
    crime_violent: pd.DataFrame,    # (date, zipcode, crime_violent)
    crime_property: pd.DataFrame,   # (date, zipcode, crime_property)
) -> pd.DataFrame:
    """
    combine dataframes into one master dataframe.
    Needed to for feature engineering.
    """

    # Join by zip code

    # Join by date (month / year)

    return dataframe