"""
Database connection manager.
Wraps the existing real_estate.db SQLite database.
"""

import sqlite3
import pandas as pd
from pathlib import Path


class DatabaseManager:
    """Thin wrapper around SQLite for the API layer."""

    def __init__(self):
        self.conn: sqlite3.Connection | None = None
        self.db_path: Path | None = None

    def connect(self, db_path: str | Path = "real_estate.db"):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Database not found at {self.db_path.resolve()}. "
                "Run the data pipeline first (main.py menu options 1-4, then load DB)."
            )
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.row_factory = sqlite3.Row
        print(f"[API DB] Connected to {self.db_path.resolve()}")

    def is_connected(self) -> bool:
        return self.conn is not None

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        """Run SQL and return list of dicts (JSON-friendly)."""
        cursor = self.conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def query_df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Run SQL and return a pandas DataFrame (for model inference)."""
        return pd.read_sql_query(sql, self.conn, params=params)

    def get_zipcodes(self) -> list[str]:
        rows = self.query("SELECT DISTINCT zipcode FROM zhvi ORDER BY zipcode")
        return [r["zipcode"] for r in rows]

    def get_zhvi(self, zipcode: str) -> list[dict]:
        return self.query(
            "SELECT year, month, zhvi FROM zhvi WHERE zipcode = ? ORDER BY year, month",
            (str(zipcode).zfill(5),),
        )

    def get_master(self, zipcode: str) -> list[dict]:
        return self.query(
            "SELECT * FROM master WHERE zipcode = ? ORDER BY year, month",
            (str(zipcode).zfill(5),),
        )

    def get_latest_master_row(self, zipcode: str) -> dict | None:
        """Get the most recent row from master for a zipcode (for predictions)."""
        rows = self.query(
            "SELECT * FROM master WHERE zipcode = ? ORDER BY year DESC, month DESC LIMIT 1",
            (str(zipcode).zfill(5),),
        )
        return rows[0] if rows else None

    def get_table_counts(self) -> dict:
        tables = self.query(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        counts = {}
        for t in tables:
            name = t["name"]
            result = self.query(f"SELECT COUNT(*) as cnt FROM {name}")
            counts[name] = result[0]["cnt"]
        return counts

    def close(self):
        if self.conn:
            self.conn.close()
            print("[API DB] Connection closed.")


# Singleton — shared across all requests
db_manager = DatabaseManager()
