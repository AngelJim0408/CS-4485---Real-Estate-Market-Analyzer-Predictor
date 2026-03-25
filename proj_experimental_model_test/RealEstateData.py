import pandas as pd
from datetime import date

class RealEstateDataClass:
    def __init__(self, data_source, data_normalize, data_engineering, year_earliest):
        self.ds = data_source
        self.dn = data_normalize
        self.de = data_engineering

        self.data_yr = date.today().year - 1 
        self.year_start = year_earliest
        self.year_end = self.data_yr - 2

        # Raw Data
        self.zhvi_df = None

        self.sales_df = None
        self.rent_df = None
        self.listings_df = None
        self.inventory_df = None
        self.redfin_alt_supply = None

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
        self.zhvi_proc = None
        self.sales_proc = None
        self.rent_proc = None
        self.listings_proc = None
        self.inventory_proc = None
        self.mortgage_rates_proc = None
        self.unemployment_rates_proc = None
        self.median_income_proc = None
        self.school_ratings_proc = None
        self.crime_violent_proc = None
        self.crime_property_proc = None

        # Master Dataframe (Fully combined from above)
        self.master_df = None

    def load_data(self):
        
        # Get data from data sources.
        # Access Other Data Tables
        self.school_directory = self.ds.get_campus_zip_data()

        self.agency_city_lookup = self.ds.get_lookup_table("agency_city.csv")
        self.zipcode_city_lookup = self.ds.get_lookup_table("zipcode_city.csv")
        self.zipcodes_lookup = self.ds.get_lookup_table("zipcodes.csv")

        # 1.PRICE MOMENTUM
        ## Get ZHVI from Zillow
        self.zhvi_df = self.ds.get_zhvi_data() # Can calculate lag, growth, volatility from this data

        # 2.SUPPLY AND DEMAND (WARNING: MANY DON'T HAVE DATA PRIOR TO 2018)
        self.sales_df = self.ds.get_zillow_supply('sales_count') # metro-lvl
        self.rent_df = self.ds.get_zillow_supply('rent') # zip-lvl
        self.listings_df = self.ds.get_zillow_supply('new_listings') # metro-lvl
        self.inventory_df = self.ds.get_zillow_supply('inventory') # metro-lvl
        self.redfin_alt_supply = self.ds.get_redfin(self.zipcodes_lookup )

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
            self.crime_violent_dict[year]['month'] = split_date[0]

            split_date = self.crime_property_dict[year]['month'].str.split('-', expand=True)
            self.crime_property_dict[year]['month'] = split_date[0]

        # 6.SEASON/CLIMATE/Quarter
        ## Since all data already come with dates, we do not need to 'collect' months/quarters.
        ## If want to see what each dataframe as, print it (just print the head, since df can have too large values)

        return self

    def process_data(self, main_folder):
        ## Process school ratings, combine into single dataframe, keep keys consistent.
        
        agency_zipcodes = pd.merge(self.zipcode_city_lookup, self.agency_city_lookup, how="left", on='city')
        agency_zipcodes = agency_zipcodes[['zipcode','agency']]

        # Normalize already combined data
        self.zhvi_proc = self.dn.normalize_zillow_data(self.zhvi_df,'zhvi')
        self.sales_proc = self.dn.normalize_zillow_data(self.sales_df,'sales_count')
        self.rent_proc = self.dn.normalize_zillow_data(self.rent_df,'rent')
        self.listings_proc = self.dn.normalize_zillow_data(self.listings_df,'new_listings')
        self.inventory_proc = self.dn.normalize_zillow_data(self.inventory_df,'inventory')
        self.redfin_alt_proc = self.dn.normalize_redfin_data(self.redfin_alt_supply)

        self.mortgage_rates_proc = self.dn.normalize_mortgage(self.mortgage_rates_df)
        self.unemployment_rates_proc = self.unemployment_rates_df # already in usable form: year,month,unemployment_rate
        # Normalize separated year data ( median income, school, crime)
        for year in range(self.year_start, self.data_yr):

            self.median_income_dict[year] = self.dn.normalize_income(self.median_income_dict[year])

            if self.school_rating_dict[year] is not None:
                year_archive = max(2019, year)
                self.school_rating_dict[year] = self.dn.normalize_school(self.school_rating_dict[year], self.school_archived_dir[year_archive])

            self.crime_violent_dict[year] = self.dn.normalize_crime(self.crime_violent_dict[year], agency_zipcodes)
            self.crime_property_dict[year] = self.dn.normalize_crime(self.crime_property_dict[year], agency_zipcodes)

        self.median_income_proc = self.dn.flatten_dataframes(self.median_income_dict)
        self.school_ratings_proc = self.dn.flatten_dataframes(self.school_rating_dict)
        self.crime_violent_proc = self.dn.flatten_dataframes(self.crime_violent_dict)
        self.crime_property_proc = self.dn.flatten_dataframes(self.crime_property_dict)

        # Reorder columns
        self.median_income_proc = self.median_income_proc[['zipcode','year','median_income','total_population']]
        self.school_ratings_proc = self.school_ratings_proc[['zipcode','campus_id','year','score']]
        self.crime_violent_proc = self.crime_violent_proc[['zipcode','agency','year','month','offenses_per_100k','offenses','clearances']]
        self.crime_property_proc = self.crime_property_proc[['zipcode','agency','year','month','offenses_per_100k','offenses','clearances']]

        # write processed to data_proc
        self.zhvi_proc.to_csv(main_folder / "data_proc/zhvi_processed.csv",index=False)
        self.sales_proc.to_csv(main_folder / "data_proc/sales_processed.csv",index=False)
        self.rent_proc.to_csv(main_folder / "data_proc/rent_processed.csv",index=False)
        self.listings_proc.to_csv(main_folder / "data_proc/listings_processed.csv",index=False)
        self.inventory_proc.to_csv(main_folder / "data_proc/inventory_processed.csv",index=False)
        self.redfin_alt_proc.to_csv(main_folder / "data_proc/redfin_alt_processed.csv",index=False)
        
        self.mortgage_rates_proc.to_csv(main_folder / "data_proc/mortgage_rates_processed.csv",index=False)
        self.unemployment_rates_proc.to_csv(main_folder / "data_proc/unemployment_rates_processed.csv",index=False)
        self.median_income_proc.to_csv(main_folder / "data_proc/median_income_processed.csv",index=False)
        self.school_ratings_proc.to_csv(main_folder / "data_proc/school_ratings_processed.csv",index=False)
        self.crime_violent_proc.to_csv(main_folder / "data_proc/crime_violent_processed.csv",index=False)
        self.crime_property_proc.to_csv(main_folder / "data_proc/crime_property_processed.csv",index=False)

        
        return self
    
    def get_processed_data(self, main_folder):
        """
        Get processed data from files if already exists (for easy use.)
        """
        self.zhvi_proc = pd.read_csv(main_folder / "data_proc/zhvi_processed.csv")
        self.sales_proc = pd.read_csv(main_folder / "data_proc/sales_processed.csv")
        self.rent_proc = pd.read_csv(main_folder / "data_proc/rent_processed.csv")
        self.listings_proc = pd.read_csv(main_folder / "data_proc/listings_processed.csv")
        self.inventory_proc = pd.read_csv(main_folder / "data_proc/inventory_processed.csv")
        self.redfin_alt_proc = pd.read_csv(main_folder / "data_proc/redfin_alt_processed.csv")
        
        self.mortgage_rates_proc = pd.read_csv(main_folder / "data_proc/mortgage_rates_processed.csv")
        self.unemployment_rates_proc = pd.read_csv(main_folder / "data_proc/unemployment_rates_processed.csv")
        self.median_income_proc = pd.read_csv(main_folder / "data_proc/median_income_processed.csv")
        self.school_ratings_proc = pd.read_csv(main_folder / "data_proc/school_ratings_processed.csv")
        self.crime_violent_proc = pd.read_csv(main_folder / "data_proc/crime_violent_processed.csv")
        self.crime_property_proc = pd.read_csv(main_folder / "data_proc/crime_property_processed.csv")
        
        return self
    
    def save_main_df(self, main_folder):
        if self.master_df is not None:
            self.master_df.to_csv(main_folder / "data_proc/MASTER.csv",index=False)

        return self
    
    def build_features(self, main_folder):
        print("Merging processed dataframes.")
        self.master_df = self.dn.build_merged_df(self.zhvi_proc,self.sales_proc,self.rent_proc,self.listings_proc,self.inventory_proc,self.redfin_alt_proc,
            self.mortgage_rates_proc,self.unemployment_rates_proc,self.median_income_proc,self.school_ratings_proc,
            self.crime_violent_proc,self.crime_property_proc
        )

        # ! drop rent column for now (too much nan values [>40%!!!])
        self.master_df = self.master_df.drop(columns=['rent'])
        self.dn.print_merged_log(self.master_df)

        self.save_main_df(main_folder)

        #Build feature vectors for model training.
        self.master_df = self.de.create_feature_vectors(self.master_df)
        #self.dn.print_merged_log(self.master_df)


        return self
    
    def get_model_inputs(self, target_str, cutoff):
        """
        Model ready data (training/test split)
        target_str: target_zhvi_3m_pct, target_zhvi_6m+pct, target_zhvi_3m, target_zhvi_6m
        returns: x_train, x_test, y_train, y_test 
        """
        # Get training and test split (by cutoff year)
        return self.de.get_train_test_split(self.master_df, target_str, cutoff)
    
    def desc(self):
        # Log to make sure dataframes are correct.
        for name, obj in self.__dict__.items():
            if isinstance(obj, pd.DataFrame):
                print(f"{name:20s}  {obj.shape}")
            elif isinstance(obj, dict):
                print(f"{name:20s}  dict with {len(obj)} keys: {list(obj.keys())[:5]}")


## Agencies (TX/Dallas County) (left here for reference)
"""
if Path(path_crime_data_raw / f"fbi_data/fbi_agencies.csv").exists():
    crime_agencies = pd.read_csv(path_crime_data_raw / f"fbi_data/fbi_agencies.csv")
else:
    crime_agencies = ds.pull_fbi_agencies("TX")

"""
