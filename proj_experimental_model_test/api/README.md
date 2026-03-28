# Real Estate Market Analyzer — API Backend

## Where this fits

This `api/` folder sits inside `proj_experimental_model_test/` alongside the existing code:

```
proj_experimental_model_test/
├── api/                    ← NEW (this folder)
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
├── data_proc/              # existing processed CSVs
├── saved_models/           # existing .joblib models
├── real_estate.db          # existing SQLite database
├── database.py             # Martin's existing DB class
├── model.py                # Martin's existing model code
└── ...
```

## Setup

```bash
cd proj_experimental_model_test
pip install fastapi uvicorn joblib scikit-learn pandas

# Run the server
uvicorn api.main:app --reload --port 8000
```

## Endpoints

| Method | Endpoint                        | Description                          |
|--------|---------------------------------|--------------------------------------|
| GET    | /api/zipcodes                   | List all available zip codes         |
| GET    | /api/zhvi/{zipcode}             | Historical home values for a zipcode |
| GET    | /api/market/{zipcode}           | Full market snapshot from master table |
| GET    | /api/predictions/{zipcode}      | 3-month and 6-month ZHVI predictions |
| GET    | /health                        | Health check                         |

## Notes

- The API reads from the existing `real_estate.db` SQLite database
- Predictions load the saved `.joblib` Random Forest models
- CORS is enabled for local React development (localhost:3000)
- Run data pipeline (menu options 1-4) before starting the API
