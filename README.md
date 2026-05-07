# CS4485 DALLASHOMES: Real-Estate-Market-Analyzer-Predictor
This program is a real estate market analyzer and prediction tool that utilizes a Random Forest Machine Learning Model to generate predictions for ZHVI (Zillow Home Value Index) based on data assigned to each index in Dallas County. Users access a webpage that showcases predictions for each zip code selection for 3 month or 6 month zhvi, with additional information on features and factors utilized to make those predictions. Additionally, users can analyze trend graphs for zhvi and other features in order to make comparisons between different zip code regions to determine the best area of the real estate market to begin searching in.

# Working with proj_experimental_model_test
Github will not have enough space to host larger datasets, thus these datasets will have to be downloaded on your own to place in the **data_raw** folder.

Additional Needs:
```bash
pip install sodapy (this is used for the Socrata API some of the websites use for data access)
pip install python-dotenv
pip install requests
pip install openpyxl
pip install -U scikit-learn
pip install fastapi uvicorn joblib scikit-learn pandas
uvicorn api.main:app --reload --port 8000 # if want to run fastapi in local.
```


[Google Drive Folder for data_raw folder](https://drive.google.com/drive/folders/1seUFRsYqhGi5qOp8wu10wKWhvJhGiJOq?usp=sharing) (Use if APIs don't work right now or for testing.)

Download the Entire folder named "data_raw", and place the folder in folder: proj_experimental_model_test

# Code Documentation
- [Data Collection](#data-collection)
- [Data Normalization](#data-normalization)
- [Feature Engineering](#feature-engineering)
- [ML Model](#ml-model)
- [Real Estate Data Class](#real-estate-data-class)
- [Data Access Service](#data-access-service)
- [FastAPI](#fastapi)
- [Testing Programs](#testing-programs)


## Data Collection
### data_source.py
- Holds functions to pull data from online APIs and websites.
- Use API key from FBI CDE API (key is free, can get on your own and update in your own .env file) ex: FBI_API_KEY=yourAPIkeyHERE
- Census API does not require key.

### api keys
API Keys, create a file called .env in the folder "proj_experimental_model_test"
Type into it: "FBI_API_KEY=yourAPIkeyHere" in the file
- Reminder: replace yourAPIkeyHere with your own API key for the FBI Crime Data Explorer, it is free and just requires your e-mail.

Currently, collects data from 2011 to (current year).
Data Organized by *Weeks*: mortgage rates
Data Organized by *Months*: crime (property & violent), unemployment rates, inventory, new listings, rent, sales
Data Organized by *Year*: medium income & population, school rating

Functions are formatted in 3 separate ways: Helper, Pull, Get.
*Helper* functions: functions utilized for repeat tasks in the data_source service.
*Pull* functions: Called when we do not have data yet or want new updated data. Call external APIs to update raw data to more recent data. 
```python
# example
def pull_zhvi_data() # directly calls zillow dataset API to get zhvi data
```
*Get* functions: Called when we just want raw data we already have in the database. 
```python
# example
def get_zhvi_data() # gets zhvi data from database into python dataframes
```

## Data Normalization
### data_normalize.py
Program will go through the raw dataframes and preform operations to make dataframes easy to use, modify, and eventually be converted into feature vectors for the training model. This includes combining separated dataframes by year into a singular dataframe with an additional column for years.
Things needed to consider
- ensure key names (column names) are lowercase and snake case
- ensure primary keys are consistent (important keys need to be the same: zipcode, year, month, region, etc.)
- convert wide rows (such as from zillow data) into normalized variants
- forward fill any empty months/dates (some data only have yearly data, so it must incorporate months so be able 
to merge with other data)

Functions: normalize functions for each data type in database, database flattening helper, build merged dataframe.
*Normalize* functions: functions that normalize a specific data type based on zipcode if applicable, year, and month.
*Flatten Dataframes* function: combines dataframes in a dictionary into a single dataframe organized by year.
*Build Merged Dataframe* function: combines all processed dataframes into one to be ready for feature engineering.

## Feature Engineering
### data_engineering.py
From the processed data, program will create features to be used in the random forest model to be able to train on the data.

#### Functions:
- *forward_fill_zip*: if empty or nan values, forward fill values (only within zipcode key)
- *create_feature_vectors*: create extra features necessary for model training from master dataframe.
- *get_master_df*: load data from database service into master dataframe.
- *clean_features_predict*: clean or remove unnecessary columns from feature vector so it can be used for model training.
- *get_train_test_split*: create training and testing split dataframes based on complete feature vector dataframe.

## ML Model
### model.py
Functionality useful for training and evaluating the random forest model. 

#### Important Functions:
- *train_model* : trains random forest model based on input dataframes and set hyperparameters.
- *eval_model* : validate model by using test split and evaluating mean absolute error, root mean square error, r2 score, and mean absolute percent error.
- *feature_analyze* : returns a list of at most 20 features, ordered in descending order of importance in relation to training the model.
- *save_model* : saves the trained model as a joblib file in saved_models folder.
- *load_model* : loads trained model from saved_models folder.

## Real Estate Data Class
### RealEstateData.py
Holds real estate data as a class/object. Able to access raw data and manipulate/process the data via functionality listed above. Additionally able to save to database by calling own object of Data Access service.

## Data Access Service 
### database.py
Holds functionality to query and insert into the sqlite database. Any function/object that requires database access needs to call this service or an object of this service.

---
# FastAPI
### api folder
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

---

# Testing Programs
## main.py
Run this program once, program will prompt for user input with menu options. User types a number to complete a menu action. Or type 'q' to quit program.
Program will initialize a new object to store dataframes and perform operations on those dataframes.

## user_predict.py
This program is used to demo/test the backend functionality to provide valid predictions within the command line and without requiring the frontend inputs or api calls.

