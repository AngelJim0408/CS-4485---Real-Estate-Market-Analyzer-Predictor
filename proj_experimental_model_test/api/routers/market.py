"""
GET /api/market/{zipcode} — Full market snapshot from the master table.
GET /api/market/{zipcode}/latest — Most recent data point for dashboard cards.
"""

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from api.database import db_manager

router = APIRouter()


def _safe_float(value) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


@router.get("/market/{zipcode}")
def get_market_data(
    zipcode: str,
    year_start: int | None = Query(None, description="Filter: start year"),
    year_end: int | None = Query(None, description="Filter: end year"),
):
    """Return the full market history for a zip code."""
    zipcode = zipcode.zfill(5)

    rows = db_manager.get_master(zipcode)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No market data found for zipcode {zipcode}",
        )

    # Apply year filters
    if year_start:
        rows = [r for r in rows if r["year"] >= year_start]
    if year_end:
        rows = [r for r in rows if r["year"] <= year_end]

    return {
        "zipcode": zipcode,
        "count": len(rows),
        "records": rows,
    }


@router.get("/market/{zipcode}/latest")
def get_latest_market(zipcode: str):
    """
    Return the most recent market data point for a zip code.
    Response shape matches what JJ's frontend expects.
    """
    zipcode = zipcode.zfill(5)

    row = db_manager.get_latest_master_row(zipcode)

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No market data found for zipcode {zipcode}",
        )

    return {
        "zipcode": zipcode,
        "year": row.get("year"),
        "month": row.get("month"),
        "current_zhvi": _safe_float(row.get("zhvi")),
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
