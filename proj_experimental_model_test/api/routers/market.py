"""
GET /api/market/{zipcode} — Full market snapshot from the master table.
Returns all merged data: home values, supply/demand, economic indicators,
school ratings, crime stats — everything the frontend needs in one call.
"""

from fastapi import APIRouter, HTTPException, Query
from api.database import db_manager
from api.models import MarketResponse, MarketRecord

router = APIRouter()


# Fields to include in the market response (subset of master table)
MARKET_FIELDS = [
    "year", "month", "zhvi",
    "sales_count", "new_listings", "inventory",
    "mortgage_rate", "unemployment_rate",
    "median_income", "total_population",
    "school_rating_mean", "school_count",
    "violent_offenses_per_100k", "property_offenses_per_100k",
]


@router.get("/market/{zipcode}", response_model=MarketResponse)
def get_market_data(
    zipcode: str,
    year_start: int | None = Query(None, description="Filter: start year"),
    year_end: int | None = Query(None, description="Filter: end year"),
):
    """
    Return the full market snapshot for a zip code.
    Pulls from the master table which merges all data sources.
    """
    zipcode = zipcode.zfill(5)

    rows = db_manager.get_master(zipcode)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No market data found for zipcode {zipcode}",
        )

    # Filter to relevant fields only
    records = []
    for row in rows:
        filtered = {k: row.get(k) for k in MARKET_FIELDS}

        # Apply year filters
        if year_start and filtered["year"] < year_start:
            continue
        if year_end and filtered["year"] > year_end:
            continue

        records.append(MarketRecord(**filtered))

    return MarketResponse(
        zipcode=zipcode,
        records=records,
        count=len(records),
    )


@router.get("/market/{zipcode}/latest")
def get_latest_market(zipcode: str):
    """
    Return only the most recent market data point for a zip code.
    Useful for dashboard summary cards.
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
        "zhvi": row.get("zhvi"),
        "sales_count": row.get("sales_count"),
        "inventory": row.get("inventory"),
        "mortgage_rate": row.get("mortgage_rate"),
        "unemployment_rate": row.get("unemployment_rate"),
        "median_income": row.get("median_income"),
        "school_rating_mean": row.get("school_rating_mean"),
        "violent_offenses_per_100k": row.get("violent_offenses_per_100k"),
        "property_offenses_per_100k": row.get("property_offenses_per_100k"),
    }
