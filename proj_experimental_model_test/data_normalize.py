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