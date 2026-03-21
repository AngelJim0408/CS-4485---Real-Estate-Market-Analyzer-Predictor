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

def fill_yearly_data():
    return

def normalize_school(school_df: pd.DataFrame, school_dir: pd.DataFrame):
    """
    Normalize school data by adding zip codes and removing unnecessary columns.
    """
    school_df = pd.merge(school_df, school_dir,on='campus_id',how='left')
    school_df = school_df[['zipcode','score','campus_id','campus']]
    school_df = school_df[school_df['score'] != '.']
    school_df['zipcode'] =  school_df['zipcode'].str[:5]

    return school_df

def build_merged_df(
    zhvi: pd.DataFrame,             # (zipcode, year, month, zhvi)
    sales: pd.DataFrame,            # (year, month, sales_count)
    rent: pd.DataFrame,             # (zipcode, year, month, rent)
    listings: pd.DataFrame,         # (year, month, listings)
    inventory: pd.DataFrame,        # (year, month, inventory)
    mortgage: pd.DataFrame,         # (year, month, mortgage_rate)
    unemployment: pd.DataFrame,     # (year, month, unemployment_rate)
    income: pd.DataFrame,           # (zipcode, year, month, median_income)  — already ffilled to monthly
    school: pd.DataFrame,           # (zipcode, year, month, school_rating)
    crime_violent: pd.DataFrame,    # (zipcode, year, month, crime_violent)
    crime_property: pd.DataFrame,   # (zipcode, year, month, crime_property)
) -> pd.DataFrame:
    
    """
    combine dataframes into one master dataframe.
    Needed to for feature engineering.
    """
    main_df = zhvi.copy()

    # Join by zip code

    # Join by date (month / year)

    return main_df