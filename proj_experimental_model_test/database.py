"""
Tables
------
  zhvi              (zipcode, year, month, zhvi)
  sales             (year, month, sales_count)
  rent              (zipcode, year, month, rent)
  listings          (year, month, new_listings)
  inventory         (year, month, inventory)
  mortgage_rates    (year, month, mortgage_rate)
  unemployment      (year, month, unemployment_rate)
  median_income     (zipcode, year, median_income, total_population)
  school_ratings    (zipcode, campus_id, year, score)
  crime_violent     (zipcode, agency, year, month, offenses_per_100k, offenses, clearances)
  crime_property    (zipcode, agency, year, month, offenses_per_100k, offenses, clearances)
  master            (all merged + engineered features — written from MASTER.csv)
"""

import sqlite3
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS zhvi (
    zipcode     TEXT    NOT NULL,
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    zhvi        REAL,
    PRIMARY KEY (zipcode, year, month)
);

CREATE TABLE IF NOT EXISTS mortgage_rates (
    year          INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    mortgage_rate REAL,
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS unemployment (
    year               INTEGER NOT NULL,
    month              INTEGER NOT NULL,
    unemployment_rate  REAL,
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS median_income (
    zipcode          TEXT    NOT NULL,
    year             INTEGER NOT NULL,
    median_income    REAL,
    total_population REAL,
    PRIMARY KEY (zipcode, year)
);

CREATE TABLE IF NOT EXISTS school_ratings (
    zipcode    TEXT    NOT NULL,
    campus_id  TEXT    NOT NULL,
    year       INTEGER NOT NULL,
    score      REAL,
    PRIMARY KEY (zipcode, campus_id, year)
);

CREATE TABLE IF NOT EXISTS crime_violent (
    zipcode            TEXT    NOT NULL,
    agency             TEXT    NOT NULL,
    year               INTEGER NOT NULL,
    month              INTEGER NOT NULL,
    offenses_per_100k  REAL,
    offenses           REAL,
    clearances         REAL,
    PRIMARY KEY (zipcode, agency, year, month)
);

CREATE TABLE IF NOT EXISTS crime_property (
    zipcode            TEXT    NOT NULL,
    agency             TEXT    NOT NULL,
    year               INTEGER NOT NULL,
    month              INTEGER NOT NULL,
    offenses_per_100k  REAL,
    offenses           REAL,
    clearances         REAL,
    PRIMARY KEY (zipcode, agency, year, month)
);

CREATE TABLE IF NOT EXISTS master (
    zipcode                     TEXT,
    year                        INTEGER,
    month                       INTEGER,

    -- Price
    zhvi                        REAL,

    -- Supply / Demand
    sales_count                 REAL,
    new_listings                REAL,
    inventory                   REAL,

    -- Macro
    mortgage_rate               REAL,
    unemployment_rate           REAL,

    -- Demographics
    median_income               REAL,
    total_population            REAL,

    -- Schools
    school_rating_mean          REAL,
    school_rating_max           REAL,
    school_count                REAL,

    -- Crime
    violent_offenses            REAL,
    violent_clearances          REAL,
    violent_offenses_per_100k   REAL,
    property_offenses           REAL,
    property_clearances         REAL,
    property_offenses_per_100k  REAL,

    PRIMARY KEY (zipcode, year, month)
);

"""


# ---------------------------------------------------------------------------
# Helper: map processed CSV filenames → table names
# ---------------------------------------------------------------------------

CSV_TABLE_MAP = {
    "zhvi_processed.csv":           "zhvi",
    "sales_processed.csv":          "sales",
    "rent_processed.csv":           "rent",
    "listings_processed.csv":       "listings",
    "inventory_processed.csv":      "inventory",
    "mortgage_rates_processed.csv": "mortgage_rates",
    "unemployment_rates_processed.csv": "unemployment",
    "median_income_processed.csv":  "median_income",
    "school_ratings_processed.csv": "school_ratings",
    "crime_violent_processed.csv":  "crime_violent",
    "crime_property_processed.csv": "crime_property",
    "MASTER.csv":                   "master",
}


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class RealEstateDB:

    def __init__(self, db_path: str | Path = "real_estate.db"):
        """
        Opens (or creates) the SQLite database at db_path.
        Call create_tables() after this to set up the schema.
        """
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")   # better write performance
        self.conn.execute("PRAGMA foreign_keys=ON")
        print(f"[DB] Connected to {self.db_path.resolve()}")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def create_tables(self):
        """Creates all tables (safe to call repeatedly — IF NOT EXISTS)."""
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        print("[DB] Tables created (or already exist).")

    # ------------------------------------------------------------------
    # Loading helpers
    # ------------------------------------------------------------------

    def _upsert_df(self, df: pd.DataFrame, table: str, if_exists: str="replace"):
        """
        Writes a DataFrame into the given table using INSERT OR REPLACE
        so re-running never causes duplicate-key errors.
        """
        if df is None or df.empty:
            print(f"[DB] Skipping {table} — DataFrame is empty or None.")
            return

        df = df.copy()

        # Ensure zipcode is always a zero-padded string where present
        if "zipcode" in df.columns:
            df["zipcode"] = df["zipcode"].astype(str).str.zfill(5)

        row_count = len(df)
        df.to_sql(table, self.conn, if_exists=if_exists, index=False,
                  method="multi", chunksize=500)
        self.conn.commit()
        print(f"[DB] {table:25s} → {row_count:,} rows written.")

    # ------------------------------------------------------------------
    # Load from CSV files (data_proc/ folder)
    # ------------------------------------------------------------------

    def load_from_csvs(self, main_folder: str | Path):
        """
        Reads every processed CSV from <main_folder>/data_proc/ and
        upserts it into the matching database table.

        Skips files that don't exist yet (e.g. MASTER.csv before step 4).
        """
        data_proc = Path(main_folder) / "data_proc"

        for filename, table in CSV_TABLE_MAP.items():
            csv_path = data_proc / filename
            if not csv_path.exists():
                print(f"[DB] Skipping {filename} — file not found.")
                continue
            df = pd.read_csv(csv_path)
            self._upsert_df(df, table)

        print("[DB] load_from_csvs complete.")

    # ------------------------------------------------------------------
    # Load directly from RealEstateDataClass (in-memory)
    # ------------------------------------------------------------------

    def load_from_class(self, data_class):
        """
        Loads processed DataFrames directly from a RealEstateDataClass
        instance (no CSV round-trip required).

        Call after data_class.process_data() or data_class.get_processed_data().
        """
        mapping = {
            "zhvi":           data_class.zhvi_proc,
            "sales":          data_class.sales_proc,
            "rent":           data_class.rent_proc,
            "listings":       data_class.listings_proc,
            "inventory":      data_class.inventory_proc,
            "mortgage_rates": data_class.mortgage_rates_proc,
            "unemployment":   data_class.unemployment_rates_proc,
            "median_income":  data_class.median_income_proc,
            "school_ratings": data_class.school_ratings_proc,
            "crime_violent":  data_class.crime_violent_proc,
            "crime_property": data_class.crime_property_proc,
            "master":         data_class.master_df,
        }

        for table, df in mapping.items():
            self._upsert_df(df, table)

        print("[DB] load_from_class complete.")

    # ------------------------------------------------------------------
    # Convenience query helpers
    # ------------------------------------------------------------------

    def query(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Run any SQL and return results as a DataFrame."""
        return pd.read_sql_query(sql, self.conn, params=params)

    def get_zipcodes(self) -> list:
        """Return sorted list of all zipcodes in the zhvi table."""
        df = self.query("SELECT DISTINCT zipcode FROM zhvi ORDER BY zipcode")
        return df["zipcode"].tolist()

    def get_zhvi_for_zip(self, zipcode: str) -> pd.DataFrame:
        return self.query(
            "SELECT year, month, zhvi FROM zhvi WHERE zipcode = ? ORDER BY year, month",
            (str(zipcode).zfill(5),)
        )

    def get_master_for_zip(self, zipcode: str) -> pd.DataFrame:
        return self.query(
            "SELECT * FROM master WHERE zipcode = ? ORDER BY year, month",
            (str(zipcode).zfill(5),)
        )


    def table_summary(self):
        """Print row counts for every table."""
        tables = [row[0] for row in
                  self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        print("\n[DB] Table summary:")
        print(f"  {'Table':<30} {'Rows':>10}")
        print(f"  {'-'*30} {'-'*10}")
        for t in sorted(tables):
            count = self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<30} {count:>10,}")
        print()

    # ------------------------------------------------------------------
    # Close
    # ------------------------------------------------------------------

    def close(self):
        self.conn.close()
        print("[DB] Connection closed.")
