"""
Pydantic response schemas for the API.
These define the shape of every JSON response.
"""

from pydantic import BaseModel


# --- ZHVI ---

class ZHVIRecord(BaseModel):
    year: int
    month: int
    zhvi: float | None


class ZHVIResponse(BaseModel):
    zipcode: str
    records: list[ZHVIRecord]
    count: int


# --- Market Snapshot ---

class MarketRecord(BaseModel):
    year: int
    month: int
    zhvi: float | None = None
    sales_count: float | None = None
    new_listings: float | None = None
    inventory: float | None = None
    mortgage_rate: float | None = None
    unemployment_rate: float | None = None
    median_income: float | None = None
    total_population: float | None = None
    school_rating_mean: float | None = None
    school_count: float | None = None
    violent_offenses_per_100k: float | None = None
    property_offenses_per_100k: float | None = None


class MarketResponse(BaseModel):
    zipcode: str
    records: list[MarketRecord]
    count: int


# --- Predictions ---

class Forecast(BaseModel):
    predicted_value: float
    predicted_change_pct: float | None
    description: str


class PredictionResponse(BaseModel):
    zipcode: str
    current_zhvi: float | None
    latest_year: int | None
    latest_month: int | None
    forecast_3m: Forecast | None
    forecast_6m: Forecast | None


# --- Zipcodes ---

class ZipcodesResponse(BaseModel):
    zipcodes: list[str]
    count: int


# --- Health ---

class HealthResponse(BaseModel):
    status: str
    database: bool
    models_loaded: bool
