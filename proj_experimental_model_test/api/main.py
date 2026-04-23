"""
Real Estate Market Analyzer — FastAPI Entry Point
Run from proj_experimental_model_test/:
    uvicorn api.main:app --reload --port 8000
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.database import db_manager
from api.services.predictor import model_manager
from api.services.data_manager import data_manager
from api.routers import zipcodes, zhvi, market, predictions, update


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: connect DB + load models. Shutdown: close DB."""
    db_manager.connect()
    data_manager.init()
    model_manager.load_models()
    yield
    db_manager.close()


app = FastAPI(
    title="Real Estate Market Analyzer API",
    description="Backend API for Dallas County real estate predictions and market data.",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow React dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(zipcodes.router, prefix="/api", tags=["Zipcodes"])
app.include_router(zhvi.router, prefix="/api", tags=["ZHVI"])
app.include_router(market.router, prefix="/api", tags=["Market"])
app.include_router(predictions.router, prefix="/api", tags=["Predictions"])
app.include_router(update.router, prefix="/api", tags=["Update"]) 


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "database": db_manager.is_connected(),
        "models_loaded": model_manager.is_loaded(),
    }
