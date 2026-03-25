import pandas as pd

# Normalization funcs
def normalize_zillow_data(df: pd.DataFrame, value_str: str) -> pd.DataFrame:
    df = df.copy()
    key_col = df.columns[0]
    df = df.melt(id_vars=[key_col], var_name='date',value_name=value_str)
    df["date"] = pd.to_datetime(df["date"])

    df = (df.set_index("date").groupby(key_col)[value_str].resample("MS").mean().reset_index())
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    #df = df.dropna(subset=[value_str])

    if key_col == 'zipcode':
        return df[[key_col,'year','month',value_str]]
    else:
        return df[['year','month',value_str]]

def normalize_mortgage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize mortgage rates by removing unecessary columns, resampling weekly data to monthly, and separating date into month and year.
    """
    df = df.copy()
    df = df.rename(columns={"Week": "date", "US30yrFRM": "mortgage_rate"})
    df["date"] = pd.to_datetime(df["date"])

    df = df[["date", "mortgage_rate"]] # These are only columns we need
    df = df.dropna(subset=["mortgage_rate"])

    # Resamples data (group by month from month start ('MS'), then combines into mean of the month group)
    # reset index at end to be used as column again
    df = (df.set_index("date")["mortgage_rate"].resample("MS").mean().reset_index())
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month

    return df[['year','month','mortgage_rate']] # only values we want.

def normalize_income(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['median_income'] != -666666666]
    df.rename(columns={'ZCTA':'zipcode'}, inplace=True)

    return df

def normalize_school(school_df: pd.DataFrame, school_dir: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize school data by adding zip codes and removing unnecessary columns.
    """
    school_df = pd.merge(school_df, school_dir,on='campus_id',how='left')
    # zipcode,score,campus_id,campus,year
    school_df = school_df[['zipcode','score','campus_id','campus']]
    school_df = school_df[school_df['score'] != '.']
    school_df['zipcode'] =  school_df['zipcode'].str[:5]

    return school_df

def normalize_crime(df: pd.DataFrame, agency_zip: pd.DataFrame) -> pd.DataFrame:
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

# Flatten dataframes (combine dataframes if separated by year)
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

def print_merged_log(df: pd.DataFrame):
    print(f"Merged shape: {df.shape}")
    print(f"Zipcodes:     {df['zipcode'].nunique()}")
    print(f"Date range:   {df['year'].min()}-{df['month'].min():02d} → "
          f"{df['year'].max()}-{df['month'].max():02d}")
    print(f"Columns:\n{df.columns.tolist()}")
    print("_______________________________________________")
    print("_______________________________________________")
    print("Nan Percentages")
    print("__________________________________________")
    null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
    print(null_pct[null_pct > 0])

def build_merged_df( # if zipcode not column, means data for whole county
    zhvi: pd.DataFrame,             # (zipcode,year,month,zhvi)
    sales: pd.DataFrame,            # (year,month,sales_count) <- across whole county
    rent: pd.DataFrame,             # (zipcode,year,month,rent)
    listings: pd.DataFrame,         # (year,month,new_listings)
    inventory: pd.DataFrame,        # (year,month,inventory)

    mortgage: pd.DataFrame,         # *(year,month,mortgage_rate)
    unemployment: pd.DataFrame,     # *(year,month,unemployment_rate)
    income: pd.DataFrame,           # *(zipcode,year,median_income,total_population !(yearly data)
    school: pd.DataFrame,           # *(zipcode,campus_id,year,score) !(yearly data)
    crime_violent: pd.DataFrame,    # *(zipcode,agency,year,month,offenses_per_100k,offenses,clearances) 
    crime_property: pd.DataFrame,   # *(zipcode,agency,year,month,offenses_per_100k,offenses,clearances)
) -> pd.DataFrame:
    
    """
    parameters: PROCESSED DATAFRAME!
    combine dataframes into one main_df dataframe.
    Needed to for feature engineering.
    """
    main_df = zhvi.copy()

    # Join by month + zip
    main_df = main_df.merge(rent, on=['zipcode','year','month'], how='left')

    # Join by month/year (county-lvl)
    county_month_list = [sales, listings, inventory, unemployment, mortgage]
    for df in county_month_list:
        main_df = main_df.merge(df, on=['year','month'], how='left')

    # Join by year + zip
    main_df = main_df.merge(income, on=['zipcode','year'])

    ## schools can have multiple per zip, aggregate the school campuses
    school["score"] = pd.to_numeric(school["score"], errors="coerce")
    school_grouped = school.groupby(["zipcode", "year"])["score"].agg(school_rating_mean="mean",school_rating_max="max",school_count="count").reset_index()
    main_df = main_df.merge(school_grouped, on=['zipcode','year'], how='left')

    ## crime data also needs aggregation with agencies + (differentiate between violent and property)
    ## remember: agencies can be assoc w/ multiple zips
    ## helper function for crime aggr
    def aggregate_crime(df: pd.DataFrame, val_str: str):
        new_df = df.groupby(['zipcode','year','month']).agg(
                    offenses=('offenses','sum'),clearances=('clearances','sum'),offenses_per_100k=('offenses_per_100k','sum')
                ).reset_index()
        new_df = new_df.rename(columns={'offenses':f'{val_str}_offenses', 'clearances' : f'{val_str}_clearances', 'offenses_per_100k' : f'{val_str}_offenses_per_100k'})
        return new_df
    
    violent_crime_agg = aggregate_crime(crime_violent, 'violent')
    property_crime_agg = aggregate_crime(crime_property, 'property')
    
    main_df = main_df.merge(violent_crime_agg, on=['zipcode','year','month'], how='left')
    main_df = main_df.merge(property_crime_agg, on=['zipcode','year','month'], how='left')

    # Need to forward fill for data that only show yearly results
    main_df = main_df.sort_values(["zipcode", "year", "month"]).reset_index(drop=True) # sort by zipcode, then year, then month so we group these in order
    cols_to_fill = ['median_income','total_population','school_rating_mean','school_rating_max','school_count']

    main_df[cols_to_fill] = main_df.groupby('zipcode')[cols_to_fill].ffill()

    return main_df
