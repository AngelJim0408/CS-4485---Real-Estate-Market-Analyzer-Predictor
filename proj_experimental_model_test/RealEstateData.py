import pandas as pd
from datetime import date
from pathlib import Path

from database import RealEstateDB


class RealEstateDataClass:
    def __init__(self, data_source, data_normalize, data_engineering, year_earliest):
        self.ds = data_source
        self.dn = data_normalize
        self.de = data_engineering

        self.curr_yr    = date.today().year
        self.year_start = year_earliest
        self.year_end   = self.curr_yr - 3

        # Reuse the shared DB instance from data_source so we're on one connection
        self.db: RealEstateDB = data_source.db

        # Raw Data
        self.zhvi_df             = None
        #self.sales_df            = None
        #self.rent_df             = None
        #self.listings_df         = None
        #self.inventory_df        = None
        self.redfin_supply_df   = None
        self.mortgage_rates_df   = None
        self.unemployment_rates_df = None
        self.median_income_dict  = {}
        self.school_rating_dict  = {}
        self.crime_violent_dict  = {}
        self.crime_property_dict = {}

        # Lookup / reference tables
        self.school_directory    = None
        self.school_archived_dir = {}
        self.agency_city_lookup  = None
        self.zipcode_city_lookup = None
        self.zipcodes_lookup     = None

        # Processed DataFrames
        self.zhvi_proc             = None
        #self.sales_proc            = None
        #self.rent_proc             = None
        #self.listings_proc         = None
        #self.inventory_proc        = None
        self.redfin_supply_proc       = None
        self.mortgage_rates_proc   = None
        self.unemployment_rates_proc = None
        self.median_income_proc    = None
        self.school_ratings_proc   = None
        self.crime_violent_proc    = None
        self.crime_property_proc   = None

        # Master / feature-engineered DataFrame
        self.master_df = None

    # ------------------------------------------------------------------
    # DATA LOADING  (raw → memory, cached in DB via data_source)
    # ------------------------------------------------------------------
    def load_data(self):
        """Pull all raw data from sources (or DB cache) into memory."""

        # Lookup tables — small static CSVs, always from disk
        self.school_directory    = self.ds.get_campus_zip_data()
        self.agency_city_lookup  = self.ds.get_lookup_table("agency_city.csv")
        self.zipcode_city_lookup = self.ds.get_lookup_table("zipcode_city.csv")
        self.zipcodes_lookup     = self.ds.get_lookup_table("zipcodes.csv")

        # 1. Price Momentum
        self.zhvi_df = self.ds.get_zhvi_data()

        # 2. Supply & Demand
        #self.sales_df          = self.ds.get_zillow_supply('sales_count')
        #self.rent_df           = self.ds.get_zillow_supply('rent')
        #self.listings_df       = self.ds.get_zillow_supply('new_listings')
        #self.inventory_df      = self.ds.get_zillow_supply('inventory')
        self.redfin_supply_df = self.ds.get_redfin(self.zipcodes_lookup)

        # 3. Economic Environment
        self.mortgage_rates_df     = self.ds.get_mortgage_rates()
        self.unemployment_rates_df = self.ds.get_unemployment(self.curr_yr, self.year_start)

        for year in range(self.year_start, self.curr_yr):
            self.median_income_dict[year]  = self.ds.get_med_income(year)

            # 4. Neighborhood Quality
            self.school_rating_dict[year]  = self.ds.get_school_rating(year)
            self.school_archived_dir[year] = self.ds.get_campus_zip_data(year)

            # 5. Safety
            self.crime_violent_dict[year]  = self.ds.get_crimes_df(year, 'V')
            self.crime_property_dict[year] = self.ds.get_crimes_df(year, 'P')

            # Note: month parsing from "MM-YYYY" FBI format is handled
            # inside data_normalize.normalize_crime() — no action needed here.

        return self

    # ------------------------------------------------------------------
    # DATA PROCESSING  (normalize → save processed tables to DB)
    # ------------------------------------------------------------------
    def process_data(self, main_folder=None):
        """
        Normalise all raw DataFrames and write the processed versions to
        the database via RealEstateDB.load_from_class().
        main_folder kept for backward-compatibility but no longer used.
        """
        agency_zipcodes = (
            pd.merge(self.zipcode_city_lookup, self.agency_city_lookup, how="left", on='city')
            [['zipcode', 'agency']]
        )

        # Normalise
        self.zhvi_proc             = self.dn.normalize_zillow_data(self.zhvi_df, 'zhvi')
        #self.sales_proc            = self.dn.normalize_zillow_data(self.sales_df, 'sales_count')
        #self.rent_proc             = self.dn.normalize_zillow_data(self.rent_df, 'rent')
        #self.listings_proc         = self.dn.normalize_zillow_data(self.listings_df, 'new_listings')
        #self.inventory_proc        = self.dn.normalize_zillow_data(self.inventory_df, 'inventory')
        self.redfin_supply_proc       = self.dn.normalize_redfin_data(self.redfin_supply_df)
        self.mortgage_rates_proc   = self.dn.normalize_mortgage(self.mortgage_rates_df)
        self.unemployment_rates_proc = self.unemployment_rates_df  # already normalised

        for year in range(self.year_start, self.curr_yr):
            self.median_income_dict[year] = self.dn.normalize_income(self.median_income_dict[year])

            if year in self.school_rating_dict and self.school_rating_dict[year] is not None:
                year_archive = max(2019, year)
                if self.school_archived_dir.get(year_archive) is not None:
                    self.school_rating_dict[year] = self.dn.normalize_school(
                        self.school_rating_dict[year], self.school_archived_dir[year_archive]
                    )
                else:
                    print(f"Warning: No school archive found for year: {year_archive}")

            if self.crime_violent_dict[year] is not None:
                self.crime_violent_dict[year] = self.dn.normalize_crime(
                    self.crime_violent_dict[year], agency_zipcodes
                )
            if self.crime_property_dict[year] is not None:
                self.crime_property_dict[year] = self.dn.normalize_crime(
                    self.crime_property_dict[year], agency_zipcodes
                )

        # Flatten year-keyed dicts into single DataFrames
        self.median_income_proc   = self.dn.flatten_dataframes(self.median_income_dict)
        self.school_ratings_proc  = self.dn.flatten_dataframes(self.school_rating_dict)
        self.crime_violent_proc   = self.dn.flatten_dataframes(self.crime_violent_dict)
        self.crime_property_proc  = self.dn.flatten_dataframes(self.crime_property_dict)

        # Enforce consistent column order
        self.median_income_proc  = self.median_income_proc[['zipcode','year','median_income','total_population']]
        self.school_ratings_proc = self.school_ratings_proc[['zipcode','campus_id','year','score']]
        self.crime_violent_proc  = self.crime_violent_proc[['zipcode','agency','year','month','offenses_per_100k','offenses','clearances']]
        self.crime_property_proc = self.crime_property_proc[['zipcode','agency','year','month','offenses_per_100k','offenses','clearances']]

        # Write all processed DataFrames to DB in one call
        self.db.load_from_class(self)
        print("All processed data saved to database.")
        return self

    # ------------------------------------------------------------------
    # LOAD PROCESSED DATA FROM DB
    # ------------------------------------------------------------------
    def get_processed_data(self, main_folder=None):
        """
        Load all processed DataFrames from the database.
        main_folder kept for backward-compatibility but no longer used.
        """
        self.zhvi_proc               = self.db.query("SELECT * FROM zhvi")
        #self.sales_proc              = self.db.query("SELECT * FROM sales")
        #self.rent_proc               = self.db.query("SELECT * FROM rent")
        #self.listings_proc           = self.db.query("SELECT * FROM listings")
        #self.inventory_proc          = self.db.query("SELECT * FROM inventory")
        self.redfin_supply_proc      = self.db.query("SELECT * FROM redfin_supply")
        self.mortgage_rates_proc     = self.db.query("SELECT * FROM mortgage_rates")
        self.unemployment_rates_proc = self.db.query("SELECT * FROM unemployment")
        self.median_income_proc      = self.db.query("SELECT * FROM median_income")
        self.school_ratings_proc     = self.db.query("SELECT * FROM school_ratings")
        self.crime_violent_proc      = self.db.query("SELECT * FROM crime_violent")
        self.crime_property_proc     = self.db.query("SELECT * FROM crime_property")

        print("Processed data loaded from database.")
        return self
    

    # ------------------------------------------------------------------
    # MASTER / FEATURE ENGINEERING
    # ------------------------------------------------------------------
    def save_main_df(self, main_folder=None):
        """Save the master merged DataFrame to the 'master' DB table."""
        if self.master_df is not None:
            self.db._upsert_df(self.master_df, "master")
            print("[DB] Master DataFrame saved to database.")
        return self

    def build_features(self, main_folder=None):
        """Merge all processed DataFrames, engineer features, and save to DB."""
        if self.master_df is None:
            self.master_df = self.load_master_from_db()
        
        print("Merging processed dataframes.")
        self.master_df = self.dn.build_merged_df(
            self.zhvi_proc, self.redfin_supply_proc,
            self.mortgage_rates_proc, self.unemployment_rates_proc,
            self.median_income_proc, self.school_ratings_proc,
            self.crime_violent_proc, self.crime_property_proc
        )

        # Drop rent — too many NaN values (>40%)
        #self.master_df = self.master_df.drop(columns=['rent'])
        self.dn.print_merged_log(self.master_df)

        #self.save_main_df()

        # Build feature vectors for model training
        self.master_df = self.de.create_feature_vectors(self.master_df)
        self.save_main_df()

        return self

    def get_model_inputs(self, target_str, cutoff):
        """
        Returns model-ready train/test split.
        target_str: target_zhvi_3m_pct | target_zhvi_6m_pct | target_zhvi_3m | target_zhvi_6m
        """
        return self.de.get_train_test_split(self.master_df, target_str, cutoff)

    def load_master_from_db(self):
        """
        Load the master DataFrame from the DB (skips full rebuild).
        Use this in user_predict.py for fast startup.
        """
        try:
            self.master_df = self.db.query("SELECT * FROM master")
            self.master_df["zipcode"] = self.master_df["zipcode"].astype(str).str.zfill(5)
            print(f"[DB] Master DataFrame loaded ({len(self.master_df):,} rows).")
        except Exception as e:
            print(f"Master table not found in DB: {e}\nRun build_features first (option 4).")
        return self

    def desc(self):
        """Print shapes of all loaded DataFrames."""
        for name, obj in self.__dict__.items():
            if isinstance(obj, pd.DataFrame):
                print(f"{name:25s}  {obj.shape}")
            elif isinstance(obj, dict):
                print(f"{name:25s}  dict({len(obj)} keys): {list(obj.keys())[:5]}")