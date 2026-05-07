# Real Estate Market Analyzer — API Backend

## Where this fits

This `api/` folder sits inside `proj_experimental_model_test/` alongside the existing code:

```
proj_experimental_model_test/
├── api/
│   ├── __init__.py
│   ├── main.py             # FastAPI app entry point
│   ├── database.py         # DB connection (wraps existing database.py)
│   ├── models.py           # Pydantic response schemas
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── zipcodes.py     # /api/zipcodes
│   │   ├── zhvi.py         # /api/zhvi/{zipcode}
│   │   ├── market.py       # /api/market/{zipcode}
│   │   └── predictions.py  # /api/predictions/{zipcode}
│   └── services/
│       ├── __init__.py
│       └── predictor.py    # Model loading + inference logic
├── data_proc/              # processed CSVs
├── saved_models/           # trained .joblib models
├── real_estate.db          # SQLite database
├── database.py             # DB class (creates tables, loads data)
├── model.py                # Model training/evaluation
└── ...
```

## Setup

```bash
cd proj_experimental_model_test

# Install dependencies
pip install fastapi uvicorn joblib scikit-learn pandas
pip install apscheduler

# Run the data pipeline first (if not already done)
# This collects data, processes it, and builds feature vectors into MASTER.csv
# Use main.py menu options 1-4

# Run the server
uvicorn api.main:app --reload --port 8000
```

## Endpoints

| Method | Endpoint                          | Description                                    |
|--------|-----------------------------------|------------------------------------------------|
| GET    | /api/zipcodes                     | List all available zip codes                   |
| GET    | /api/zhvi/{zipcode}               | Historical home values (optional year filters) |
| GET    | /api/market/{zipcode}             | Full market snapshot from master table          |
| GET    | /api/market/{zipcode}/latest      | Most recent market data point                  |
| GET    | /api/predictions/{zipcode}        | 3-month and 6-month ZHVI predictions           |
| GET    | /health                           | Health check                                   |

The predictions endpoint accepts optional query parameters: `?year=2024&month=6`

## Notes

- The API reads from `real_estate.db` (SQLite)
- Predictions use the `feature_vectors` table which contains engineered features (lags, rolling means, ratios)
- Feature vectors are automatically included in MASTER.csv when the data pipeline runs
- Predictions use the two dollar models (target_zhvi_3m, target_zhvi_6m) — percentage change is calculated from the predictions
- CORS is enabled for all origins during development
- Interactive API docs available at http://localhost:8000/docs
