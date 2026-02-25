# CS-4485---Real-Estate-Market-Analyzer-Predictor
Develop a customized real estate marker analyzer and prediction tool that should be equipped with a graphical interface that allows user to select a region and/or time span of analysis and prediction, gather and analyze necessary data and predicts real estate market trends.

# Working with proj_experimental_model_test
Github will not have enough space to host larger datasets, thus these datasets will have to be downloaded on your own to place in the **data_raw** folder.

Additional Needs:
- pip install sodapy (this is used for the Socrata API some of the websites use for data access)

[Google Drive Folder for data_raw folder](https://drive.google.com/drive/folders/1seUFRsYqhGi5qOp8wu10wKWhvJhGiJOq?usp=sharing)

Download the Entire folder named "data_raw", and place the folder in folder: proj_experimental_model_test

Notes: Crime Data so far only from Dallas PD, may need from each city + Sheriff's Dept.

## data_source.py
- Holds functions to pull data from online websites