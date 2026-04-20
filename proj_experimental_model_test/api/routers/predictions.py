"""
GET /api/predictions/{zipcode} — Run the trained Random Forest models
against the latest data for a zip code and return price forecasts.

Reads from the feature_vectors table which contains the engineered
features (lags, rolling means, ratios) that the models were trained on.
"""

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from api.database import db_manager
from api.services.predictor import model_manager

router = APIRouter()


def _safe_float(value) -> float | None:
    """Safely convert a value to float, returning None for NaN/None."""
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


@router.get("/predictions/{zipcode}")
def get_predictions(
    zipcode: str,
    year: int | None = Query(default=None, ge=2000, le=2100, description="Target year"),
    month: int | None = Query(default=None, ge=1, le=12, description="Target month"),
):
    """
    Generate price predictions for a zip code using saved Random Forest models.

    Reads engineered features from the feature_vectors table in the database.
    If year/month are provided, predictions are based on that time period.
    Otherwise, the most recent data point is used.
    """
    zipcode = zipcode.zfill(5)

    if not model_manager.is_loaded():
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Ensure saved_models/ contains .joblib files.",
        )

    if not db_manager.has_master():
        raise HTTPException(
            status_code=503,
            detail="Feature vectors (master table) not loaded in database. master table needs to be loaded in database.",
        )

    # Get the feature row from the feature_vectors table
    row = db_manager.get_feature_row(zipcode, year, month)

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for zipcode {zipcode} with the requested year/month.",
        )

    # Get current ZHVI and market data from the row
    current_zhvi = _safe_float(row.get("zhvi"))
    row_year = row.get("year")
    row_month = row.get("month")

    # Convert to single-row DataFrame for sklearn
    features_df = pd.DataFrame([row])

    # Drop non-feature columns — same logic as clean_features_predict()
    unused_cols = ["zipcode", "year", "month"]
    target_cols = [c for c in features_df.columns if c.startswith("target_")]
    raw_cols = ["zhvi"]  # model uses lags, not raw value

    drop_cols = [c for c in unused_cols + target_cols + raw_cols if c in features_df.columns]
    features_df = features_df.drop(columns=drop_cols, errors="ignore")

    # Drop rows with NaN (model can't predict with missing features)
    features_df = features_df.dropna(axis=1, how="all")

    # Run dollar models
    pred_3m = model_manager.predict("target_zhvi_3m", features_df.copy())
    pred_6m = model_manager.predict("target_zhvi_6m", features_df.copy())

    if pred_3m is None or pred_6m is None:
        raise HTTPException(
            status_code=500,
            detail="Models failed to generate predictions. Check feature alignment.",
        )

    # Calculate percentage changes from current ZHVI
    pct_change_3m = None
    pct_change_6m = None
    if current_zhvi and current_zhvi > 0:
        pct_change_3m = round(((pred_3m - current_zhvi) / current_zhvi) * 100, 2)
        pct_change_6m = round(((pred_6m - current_zhvi) / current_zhvi) * 100, 2)

    # Get market data from master table for display
    master_row = db_manager.get_latest_master_row(zipcode)

    return {
        "zipcode": zipcode,
        "year": row_year,
        "month": row_month,
        "current_zhvi": current_zhvi,
        "prediction_3m": round(pred_3m, 2),
        "prediction_6m": round(pred_6m, 2),
        "pct_change_3m": pct_change_3m,
        "pct_change_6m": pct_change_6m,
        "market_information": {
            "mortgage_rate": _safe_float(row.get("mortgage_rate")),
            "unemployment_rate": _safe_float(row.get("unemployment_rate")),
            "median_income": _safe_float(row.get("median_income")),
            "inventory": _safe_float(row.get("inventory")),
            "sales_count": _safe_float(row.get("sales_count")),
            "new_listings": _safe_float(row.get("new_listings")),
        },
        "additional_factors": {
            "school_rating_mean": _safe_float(row.get("school_rating_mean")),
            "violent_offenses_per_100k": _safe_float(row.get("violent_offenses_per_100k")),
            "property_offenses_per_100k": _safe_float(row.get("property_offenses_per_100k")),
        },
    }
