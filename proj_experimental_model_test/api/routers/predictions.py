"""
GET /api/predictions/{zipcode} — Run the trained Random Forest models
against the latest data for a zip code and return price forecasts.

Primary predictions come from the dollar models (target_zhvi_3m, target_zhvi_6m).
Percentage models (target_zhvi_3m_pct, target_zhvi_6m_pct) are display-only,
shown alongside the dollar prediction for context.
"""

import pandas as pd
from fastapi import APIRouter, HTTPException
from api.database import db_manager
from api.services.predictor import model_manager
from api.models import PredictionResponse, Forecast

router = APIRouter()


@router.get("/predictions/{zipcode}", response_model=PredictionResponse)
def get_predictions(zipcode: str):
    """
    Generate price predictions for a zip code using saved Random Forest models.

    Returns:
        forecast_3m: Predicted home value in 3 months (from dollar model)
                     with percentage change (from pct model) for display.
        forecast_6m: Predicted home value in 6 months (from dollar model)
                     with percentage change (from pct model) for display.
    """
    zipcode = zipcode.zfill(5)

    if not model_manager.is_loaded():
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Ensure saved_models/ contains .joblib files.",
        )

    # Get the latest master row — this has all the engineered features
    latest = db_manager.get_latest_master_row(zipcode)

    if not latest:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for zipcode {zipcode}. Run data pipeline first.",
        )

    # Convert to single-row DataFrame for sklearn
    features_df = pd.DataFrame([latest])

    # Drop non-feature columns (these aren't model inputs)
    drop_cols = ["zipcode", "year", "month"]
    target_cols = [c for c in features_df.columns if c.startswith("target_")]
    features_df = features_df.drop(
        columns=[c for c in drop_cols + target_cols if c in features_df.columns],
        errors="ignore",
    )

    # Run all models
    raw_predictions = model_manager.predict_all(features_df)

    if not raw_predictions:
        raise HTTPException(
            status_code=500,
            detail="Models failed to generate predictions. Check feature alignment.",
        )

    # Build 3-month forecast: dollar model is the prediction, pct model is display-only
    forecast_3m = None
    if "target_zhvi_3m" in raw_predictions:
        forecast_3m = Forecast(
            predicted_value=round(raw_predictions["target_zhvi_3m"], 2),
            predicted_change_pct=round(raw_predictions.get("target_zhvi_3m_pct", 0), 2) if "target_zhvi_3m_pct" in raw_predictions else None,
            description="Predicted home value in 3 months",
        )

    # Build 6-month forecast: same approach
    forecast_6m = None
    if "target_zhvi_6m" in raw_predictions:
        forecast_6m = Forecast(
            predicted_value=round(raw_predictions["target_zhvi_6m"], 2),
            predicted_change_pct=round(raw_predictions.get("target_zhvi_6m_pct", 0), 2) if "target_zhvi_6m_pct" in raw_predictions else None,
            description="Predicted home value in 6 months",
        )

    return PredictionResponse(
        zipcode=zipcode,
        current_zhvi=latest.get("zhvi"),
        latest_year=latest.get("year"),
        latest_month=latest.get("month"),
        forecast_3m=forecast_3m,
        forecast_6m=forecast_6m,
    )
