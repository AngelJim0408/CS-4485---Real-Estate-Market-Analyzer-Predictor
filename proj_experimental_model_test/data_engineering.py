import pandas as pd
import numpy as np

def create_feature_vectors(df: pd.DataFrame):
    """
    Creates the features to be used in the model from the merged dataframe.
    Function must be called AFTER merging dataframes into main master dataframe.

    Features incl: lag, rolling mean/std, year-over-year % change, ratios, seasonality, and target
    """
    # Make sure dataframe is sorted by (zip, year, month)
    df = df.copy().sort_values(["zipcode", "year", "month"]).reset_index(drop=True)

    # function for each zipcode grouping
    def zip_modify(group):
        group = group.copy()
        # lag values
        """
        Gives model memory, so that price history in the past can be used to make predictions from price at ceratin date.
        """
        if 'zhvi' in group.columns:
            for lag_m in [1, 3, 6, 12]: # Memory from 1-12 months
                    group[f'zhvi_lag{lag_m}m'] = group['zhvi'].shift(lag_m)

        if 'inventory' in group.columns:
            for lag_m in [1, 3, 6]: # Memory from 1-6 months
                    group[f'inventory_lag{lag_m}m'] = group['inventory'].shift(lag_m)

        ## short time-frame triggers
        for col in ['new_listings','mortgage_rate','unemployment_rate']:
            if col in group.columns:
                for lag_m in [1, 3]:
                    group[f"{col}_lag{lag_m}m"] = group[col].shift(lag_m)


        # rolling mean/std
        """
        Lag can be sensitive on single-month data, so use rolling mean and std.
        rolling mean: represent the underlying trend
        rolling std: help capture volatility (stable prices vs swings)
        """
        for col in ['zhvi','inventory','new_listings','sales_count']:
             if col in group.columns:
                for month in [3,6]:
                    group[f"{col}_roll{month}m_mean"] = group[col].shift(1).rolling(month).mean()
                    group[f"{col}_roll{month}m_std"]  = group[col].shift(1).rolling(month).std()

        for col in ['mortgage_rate','unemployment_rate']: # values won't need std: volatility for these values not as useful.
             if col in group.columns:
                group[f"{col}_roll3m_mean"] = group[col].shift(1).rolling(3).mean() # just 3 month frame

        
        # Year-over-year % change
        """
        Compare month based on year (ex: Jan-2019 vs Jan-2020)
        Helps get true growth rate regardless of seasonality (prevent confusion w/ seasonal growth)
        """
        for col in ['zhvi','median_income','unemployment_rate','sales_count','inventory','new_listings']:
             if col in group.columns:
                  group[f'{col}_yoy_percent'] = group[col].pct_change(12) * 100

        # ratios
        """
        Gives context to raw numbers, as raw numbers by itself don't tell useful info. 
        _
        (ignore rent for now, too many NaN values)
        rent_to_price_ratio     : investor demand signal, high ratio = buy pressure     (rent : zhvi)
        _
        price_to_income_ratio   : affordability signal, predicts demand ceiling         (zhvi : median_income)
        mortgage_affordability  : affordability pressure on buyers (mortgage)           (mortgage_rate, zhvi : median_income)
        months_of_supply        : inventory signal, <3 months = seller's market         (inventory : sales_count)
        absorption_rate         : selling vs new listings introduced                    (sales_count : new_listings)
        price_to_school_rating  : price based on school quality                         (zhvi : school_rating_mean)
        price_to_crime          : zhvi relation to total crime per 100k                 (zhvi : total_offenses_per_100k)
        price_to_violent        : zhvi relation to violent crime per 100k               (zhvi : violent_offenses_per_100k)
        price_to_property       : zhvi relation to property crime per 100k              (zhvi : property_offenses_per_100k)
        """
        zhvi   = group['zhvi'] if 'zhvi' in group.columns else None
        income = group['median_income'].replace(0, pd.NA) if 'median_income' in group.columns else None

        # Price affordability
        if 'median_income' in group.columns  and zhvi is not None:
            group['price_to_income_ratio'] = zhvi / income

        # Mortgage affordability
        if all(c in group.columns for c in ['mortgage_rate', 'median_income']) and zhvi is not None: # if all columns exist
             month_rate = (group['mortgage_rate'] / 100) / 12 # convert annual rate to monthly
             loan = zhvi * 0.8  # assumes 20% down??? -> recheck later

             # standard fixed-rate mortgage payment formula
             # P: principal loan, r: monthly interest rate, n: num payments
             # [P * r * (1 + r)^n] ÷ [(1 + r)^n – 1]
             month_pay = loan * (month_rate / (1 - (1 + month_rate) ** -360))
             group['mortgage_affordability'] = month_pay / (income / 12)

        # months to sell current inv at curr sales pacing
        if all(c in group.columns for c in ['inventory', 'sales_count']):
             group['months_of_supply'] = group['inventory'] / group['sales_count'].replace(0, pd.NA)
        
        # percent of new listings being converted to sales
        if all(c in group.columns for c in ['sales_count', 'new_listings']):
             group['absorption_rate'] = group['sales_count'] / group['new_listings'].replace(0, pd.NA)

        # school quality to price relation
        if 'school_rating_mean' in group.columns and zhvi is not None:
             group['price_to_school_rating'] = zhvi / group['school_rating_mean'].replace(0, pd.NA)

        
        # crime ratio to price
        if all(c in group.columns for c in ['violent_offenses_per_100k', 'property_offenses_per_100k']) and zhvi is not None:
            group['total_offenses_per_100k'] = group['violent_offenses_per_100k'] + group['property_offenses_per_100k']

            # add 1 to offenses_per_100k : ensure nonzero division for 0 reported offesnes
            group['price_to_crime'] = zhvi / (group['total_offenses_per_100k'] + 1)
            group['price_to_violent'] = zhvi / (group['violent_offenses_per_100k'] + 1)
            group['price_to_property'] = zhvi / (group['property_offenses_per_100k'] + 1)


        # seasonality
        """
        Use sin, cos to ensure cyclic. Remember: month of January is next to December.
        Since we use cyclic months, don't need discrete season data(i.e. Winter: 12 -> 2)
        """
        group["month_sin"] = np.sin(2 * np.pi * group["month"] / 12)
        group["month_cos"] = np.cos(2 * np.pi * group["month"] / 12)
        group["quarter"]   = ((group["month"] - 1) // 3) + 1 # Reports can be done quarterly

        # target vars (this is what we're predicting)
        group["target_zhvi_3m"]  = group["zhvi"].shift(-3)
        group["target_zhvi_6m"]  = group["zhvi"].shift(-6)
        group["target_zhvi_12m"] = group["zhvi"].shift(-12)

        return group
    
    df = df.groupby("zipcode", group_keys=False).apply(zip_modify)
    return df