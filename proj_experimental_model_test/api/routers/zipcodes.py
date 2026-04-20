"""
GET /api/zipcodes — List all available Dallas County zip codes.
"""

from fastapi import APIRouter, HTTPException
from api.database import db_manager
from api.models import ZipcodesResponse

router = APIRouter()


@router.get("/zipcodes", response_model=ZipcodesResponse)
def list_zipcodes():
    """Return all zip codes that have data in the database."""
    try:
        zipcodes = db_manager.get_zipcodes()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return ZipcodesResponse(zipcodes=zipcodes, count=len(zipcodes))
