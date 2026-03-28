"""
GET /api/predictions/{zipcode} — Run the trained Random Forest models
against the latest data for a zip code and return price forecasts.
"""

import pandas as pd
from fastapi import APIRouter, HTTPException
from api.database import db_manager
from api.services.predictor import model_manager
from api.models import PredictionResponse, PredictionResult

router = APIRouter()

# Human-readable descriptions for each target
TARGET_DESCRIPTIONS = {
    "target_zhvi_3m": "Predicted home value in 3 months ($)",
    "target_zhvi_6m": "Predicted home value in 6 months ($)",
    "target_zhvi_3m_pct": "Predicted 3-month price change (%)",
    "target_zhvi_6m_pct": "Predicted 6-month price change (%)",
}


@router.get("/predictions/{zipcode}", response_model=PredictionResponse)
def get_predictions(zipcode: str):
    """
    Generate price predictions for a zip code using saved Random Forest models.

    Process:
    1. Pull the latest row from the master table for the zipcode
    2. Build a feature DataFrame matching what the models were trained on
    3. Run each model (3-month, 6-month, percentage variants)
    4. Return predictions as JSON
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

    # Format response
    predictions = []
    for target, value in raw_predictions.items():
        predictions.append(
            PredictionResult(
                target=target,
                predicted_value=round(value, 2),
                description=TARGET_DESCRIPTIONS.get(target, target),
            )
        )

    return PredictionResponse(
        zipcode=zipcode,
        current_zhvi=latest.get("zhvi"),
        latest_year=latest.get("year"),
        latest_month=latest.get("month"),
        predictions=predictions,
    )
