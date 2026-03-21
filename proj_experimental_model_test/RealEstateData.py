import pandas as pd
from datetime import date

class RealEstateDataClass:
    def __init__(self, data_source, data_normalize, year_earliest):
        self.ds = data_source
        self.dn = data_normalize

        self.data_yr = date.today().year - 1 
        self.year_start = year_earliest
        self.year_end = self.data_yr - 2

        # Raw Data
        self.zhvi_df = None

        self.sales_df = None
        self.rent_df = None
        self.listings_df = None
        self.inventory_df = None

        self.mortgage_rates_df = None
        self.unemployment_rates_df = None
        self.median_income_dict = {} # dictionary key: year, value: dataframe

        self.school_rating_dict = {}

        self.crime_violent_dict = {}
        self.crime_property_dict = {}

        # Data Tables for normalization
        self.school_directory = None
        self.school_archived_dir = {}

        self.agency_city_lookup = None
        self.zipcode_city_lookup = None
        self.zipcodes_lookup = None

        # Processed data
        self.school_ratings_proc = None
        self.crime_violent_proc = None
        self.crime_property_proc = None

    def load_data(self):
        
        # Get data from data sources.

        # 1.PRICE MOMENTUM
        ## Get ZHVI from Zillow
        self.zhvi_df = self.ds.get_zhvi_data() # Can calculate lag, growth, volatility from this data

        # 2.SUPPLY AND DEMAND
        self.sales_df = self.ds.get_zillow_supply('sales_count') # metro-lvl
        self.rent_df = self.ds.get_zillow_supply('rent') # zip-lvl
        self.listings_df = self.ds.get_zillow_supply('new_listings') # metro-lvl
        self.inventory_df = self.ds.get_zillow_supply('inventory') # metro-lvl

        # 3.ECONOMIC ENVIRONMENT
        self.mortgage_rates_df = self.ds.get_mortgage_rates() # Weekly level (based on country)
        self.unemployment_rates_df = self.ds.get_unemployment(self.data_yr) # County-lvl (Can try zip level if need be)

        for year in range(self.year_start, self.data_yr):
            self.median_income_dict[year] = self.ds.get_med_income(year)

            # 4.NEIGHBORHOOD QUALITY
            ## Get School Rating Data for every agency listed
            self.school_rating_dict[year] = self.ds.get_school_rating(year) 
            self.school_archived_dir[year] = self.ds.get_campus_zip_data(year)

            # 5.SAFETY
            ## Get Crime Data for every agency listed in "agency_city.csv"
            #   P = Property, V = Violent
            #   Get offense per 100k people, offense count, clearance count, population of jurisdiction.
            self.crime_violent_dict[year] = self.ds.get_crimes_df(year,'V')
            self.crime_property_dict[year] = self.ds.get_crimes_df(year,'P')

            split_date = self.crime_violent_dict[year]['month'].str.split('-', expand=True)
            self.crime_property_dict[year]['month'] = split_date[0]

            split_date = self.crime_property_dict[year]['month'].str.split('-', expand=True)
            self.crime_property_dict[year]['month'] = split_date[0]

        # 6.SEASON/CLIMATE/Quarter
        ## Since all data already come with dates, we do not need to 'collect' months/quarters.
        ## If want to see what each dataframe as, print it (just print the head, since df can have too large values)

        # Access Other Data Tables
        self.school_directory = self.ds.get_campus_zip_data()

        self.agency_city_lookup = self.ds.get_lookup_table("agency_city.csv")
        self.zipcode_city_lookup = self.ds.get_lookup_table("zipcode_city.csv")
        self.zipcodes_lookup = self.ds.get_lookup_table("zipcodes.csv")

        return self

    def process_data(self, main_folder):
        ## Process school ratings, combine into single dataframe, keep keys consistent.
        
        agency_zipcodes = pd.merge(self.zipcode_city_lookup, self.agency_city_lookup, how="left", on='city')
        agency_zipcodes = agency_zipcodes[['zipcode','agency']]

        # Normalize separated year data ( school, crime, ...)
        for year in range(self.year_start, self.data_yr):
            if self.school_rating_dict[year] is not None:
                year_archive = max(2019, year)
                self.school_rating_dict[year] = self.dn.normalize_school(self.school_rating_dict[year], self.school_archived_dir[year_archive])

            self.crime_violent_dict[year] = self.dn.normalize_crime(self.crime_violent_dict[year], agency_zipcodes)
            self.crime_property_dict[year] = self.dn.normalize_crime(self.crime_property_dict[year], agency_zipcodes)


        self.school_ratings_proc = self.dn.flatten_dataframes(self.school_rating_dict)

        cols = list(self.school_ratings_proc.columns)
        cols[1], cols[4] = cols[4], cols[1] # swap columns for view purposes (swap score w/ year)
        self.school_ratings_proc = self.school_ratings_proc[cols]

        self.crime_violent_proc = self.dn.flatten_dataframes(self.crime_violent_dict)
        self.crime_property_proc = self.dn.flatten_dataframes(self.crime_property_dict)

        self.school_ratings_proc.to_csv(main_folder / "data_proc/school_ratings_processed.csv",index=False)
        self.crime_violent_proc.to_csv(main_folder / "data_proc/crime_violent_processed.csv",index=False)
        self.crime_property_proc.to_csv(main_folder / "data_proc/crime_property_processed.csv",index=False)

        return self
    
    def build_features(self):
        # TODO: Build feature vectors for model training.
        return self
    
    def desc(self):
        # Log to make sure dataframes are correct.
        for name, obj in self.__dict__.items():
            if isinstance(obj, pd.DataFrame):
                print(f"{name:20s}  {obj.shape}")
            elif isinstance(obj, dict):
                print(f"{name:20s}  dict with {len(obj)} keys: {list(obj.keys())[:5]}")


## Agencies (TX/Dallas County) (left for reference)
"""
if Path(path_crime_data_raw / f"fbi_data/fbi_agencies.csv").exists():
    crime_agencies = pd.read_csv(path_crime_data_raw / f"fbi_data/fbi_agencies.csv")
else:
    crime_agencies = ds.pull_fbi_agencies("TX")

"""


"""
## Hardcode to test is dallas_crime_raw csv exists
if Path(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv").exists():
    crime_df_dallas = pd.read_csv(path_crime_data_raw / f"dallas_crime_raw_{data_yr}.csv")
else:
    crime_df_dallas = ds.pull_dallas_crime(data_yr)
"""
