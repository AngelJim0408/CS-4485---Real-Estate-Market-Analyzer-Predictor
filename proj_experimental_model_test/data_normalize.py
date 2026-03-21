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

# Normalization funcs
def normalize_school(school_df: pd.DataFrame, school_dir: pd.DataFrame):
    """
    Normalize school data by adding zip codes and removing unnecessary columns.
    """
    school_df = pd.merge(school_df, school_dir,on='campus_id',how='left')
    # zipcode,score,campus_id,campus,year
    school_df = school_df[['zipcode','score','campus_id','campus']]
    school_df = school_df[school_df['score'] != '.']
    school_df['zipcode'] =  school_df['zipcode'].str[:5]

    return school_df

def normalize_crime(df: pd.DataFrame, agency_zip: pd.DataFrame):
    """
    Normalize crime data by getting agency_city table, and zipcode city table. 
    Then assign agency to zipcode in order

    input columns: agency month  offenses_per_100k  offenses  clearances  population
    output columns: zipcode year month offenses_per_100k  offenses clearances 
    """

    """
    TODO: add zip association for highland park
    agency ids not Assoc w/ zip (will count for multiple zips if implement)
    TX0571300 Highland Park 75205, 75209, 75219	 <- keep for Real estate Data

    TX0570600 Cockrell Hill 75211			 <- entirely inside Dallas PD
    TX0572400 Wilmer 	75172			 <- small population
    TX0574700 Glenn Heights 75154			 <- Small presence
    TX0575500 Ovilla	75154			 <- Small presence
    TX0700200 Ferris	75125			 <- Mostly Ellis
    TX0610600 Lewisville	75067, 75077,75057,75056 <- Remove (Mostly Denton County)
    TX0430800 Wylie		75098 			 <- Remove (Mostly Collin County)
    """
    df = pd.merge(df,agency_zip,on='agency',how='left')
    df.dropna(subset=['zipcode'], inplace=True)
    df.dropna(subset=['offenses'], inplace=True)

    df["zipcode"] = (
        df["zipcode"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)  # remove trailing .0
        .str.zfill(5)                           # ensure 5-digit padding
    )

    df = df[['zipcode','agency','month','offenses_per_100k','offenses','clearances']]

    return df

def build_merged_df(
    zhvi: pd.DataFrame,             # (zipcode, year, month, zhvi)
    sales: pd.DataFrame,            # (year, month, sales_count)
    rent: pd.DataFrame,             # (zipcode, year, month, rent)
    listings: pd.DataFrame,         # (year, month, listings)
    inventory: pd.DataFrame,        # (year, month, inventory)
    mortgage: pd.DataFrame,         # (year, month, mortgage_rate)
    unemployment: pd.DataFrame,     # (year, month, unemployment_rate)
    income: pd.DataFrame,           # (zipcode, year, month, median_income)  — already ffilled to monthly
    school: pd.DataFrame,           # *(zipcode, year, month, school_rating) 
    crime_violent: pd.DataFrame,    # *(zipcode, year, month, crime_violent) 
    crime_property: pd.DataFrame,   # *(zipcode, year, month, crime_property)
) -> pd.DataFrame:
    
    """
    combine dataframes into one master dataframe.
    Needed to for feature engineering.
    """
    main_df = zhvi.copy()

    # Join by zip code

    # Join by date (month / year)

    return main_df