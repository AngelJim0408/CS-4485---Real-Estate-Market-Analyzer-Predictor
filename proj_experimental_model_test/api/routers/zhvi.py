"""
GET /api/zhvi/{zipcode} — Historical Zillow Home Value Index data.
Supports optional year range filtering for the frontend charts.
"""

from fastapi import APIRouter, HTTPException, Query
from api.database import db_manager
from api.models import ZHVIResponse, ZHVIRecord

router = APIRouter()


@router.get("/zhvi/{zipcode}", response_model=ZHVIResponse)
def get_zhvi(
    zipcode: str,
    year_start: int | None = Query(None, description="Filter: start year (inclusive)"),
    year_end: int | None = Query(None, description="Filter: end year (inclusive)"),
):
    """
    Return monthly ZHVI (Zillow Home Value Index) for a zip code.
    Used by the frontend to render price history charts.
    """
    zipcode = zipcode.zfill(5)

    records = db_manager.get_zhvi(zipcode)

    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No ZHVI data found for zipcode {zipcode}",
        )

    # Apply optional year filters
    if year_start is not None:
        records = [r for r in records if r["year"] >= year_start]
    if year_end is not None:
        records = [r for r in records if r["year"] <= year_end]

    return ZHVIResponse(
        zipcode=zipcode,
        records=[ZHVIRecord(**r) for r in records],
        count=len(records),
    )
