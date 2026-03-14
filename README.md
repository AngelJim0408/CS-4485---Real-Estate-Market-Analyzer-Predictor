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

## data_collect.py
Run this program once, and it will collect various data from website or API calls (some only works with physical download). All files will be stored in data_raw to be ready for processing into feature vectors.

Currently, collects data from 2018 to (current year - 1).
Data Organized by *Weeks*: mortgage rates
Data Organized by *Months*: crime (property & violent), unemployment rates, inventory, new listings, rent, sales
Data Organized by *Year*: medium income & population, school rating

API Keys, create a file called .env in the folder "proj_experimental_model_test"
Type into it: "FBI_API_KEY=yourAPIkeyHere" in the file
- Reminder: replace yourAPIkeyHere with your own API key for the FBI Crime Data Explorer, it is free and just requires your e-mail.

Note: Currently saves into csv files, later move to Database.