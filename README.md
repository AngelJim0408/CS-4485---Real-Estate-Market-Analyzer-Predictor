# CS-4485---Real-Estate-Market-Analyzer-Predictor
Develop a customized real estate marker analyzer and prediction tool that should be equipped with a graphical interface that allows user to select a region and/or time span of analysis and prediction, gather and analyze necessary data and predicts real estate market trends.

# Working with proj_experimental_model_test
Github will not have enough space to host larger datasets, thus these datasets will have to be downloaded on your own to place in the **data_raw** folder.

Additional Needs:
- pip install sodapy (this is used for the Socrata API some of the websites use for data access)
- pip install python-dotenv
- pip install requests
- pip install openpyxl

[Google Drive Folder for data_raw folder](https://drive.google.com/drive/folders/1seUFRsYqhGi5qOp8wu10wKWhvJhGiJOq?usp=sharing)

Download the Entire folder named "data_raw", and place the folder in folder: proj_experimental_model_test

Notes: Crime Data so far only from Dallas PD, may need from each city + Sheriff's Dept.

## data_source.py
- Holds functions to pull data from online websites
- Use API key from FBI CDE API (key is free, can get on your own and update in your own .env file) ex: FBI_API_KEY=yourAPIkeyHERE
- Census API does not require key.

## main.py
Run this program once, program will prompt for user input with menu options. User types a number to complete a menu action. Or type 'q' to quit program.
Program will initialize a new object to store dataframes and perform operations on those dataframes.

### 1. Collect Data
Program will get all raw data and store in dataframes in a RealEstateDataClass object. If data does not exist, will pull data from online sources. 

### 2. Process Raw Data (Unfinished)
Program will go through the raw dataframes and preform operations to make dataframes easy to use, modify, and eventually be converted into feature vectors for the training model. This includes combining separated dataframes by year into a singular dataframe with an additional column for years.
Things needed to consider
- ensure key names (column names) are lowercase and snake case
- ensure primary keys are consistent (important keys need to be the same: zipcode, year, month, region, etc.)
- convert wide rows (such as from zillow data) into normalized variants
- forward fill any empty months/dates (some data only have yearly data, so it must incorporate months so be able to merge with other data)

### 3. Feature Engineering (Not Started)
From the processed data, program will create features to be used in the random forest model to be able to train on the data.
- may need to do some final cleanup here, including dropping NaN value rows.

Currently, collects data from 2011 to (current year - 1).
Data Organized by *Weeks*: mortgage rates
Data Organized by *Months*: crime (property & violent), unemployment rates, inventory, new listings, rent, sales
Data Organized by *Year*: medium income & population, school rating

API Keys, create a file called .env in the folder "proj_experimental_model_test"
Type into it: "FBI_API_KEY=yourAPIkeyHere" in the file
- Reminder: replace yourAPIkeyHere with your own API key for the FBI Crime Data Explorer, it is free and just requires your e-mail.

