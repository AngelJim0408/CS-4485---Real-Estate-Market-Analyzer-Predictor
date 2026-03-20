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