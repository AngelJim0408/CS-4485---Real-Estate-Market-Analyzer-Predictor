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

Currently, collects data from 2018 to (current year - 1).
Data Organized by *Weeks*: mortgage rates
Data Organized by *Months*: crime (property & violent), unemployment rates, inventory, new listings, rent, sales
Data Organized by *Year*: medium income & population, school rating

API Keys, create a file called .env in the folder "proj_experimental_model_test"
Type into it: "FBI_API_KEY=yourAPIkeyHere" in the file
- Reminder: replace yourAPIkeyHere with your own API key for the FBI Crime Data Explorer, it is free and just requires your e-mail.

Note: Currently saves into csv files, later move to Database.

Following is the result of printing dataframes from raw data: 

---
### print(zhvi_df.head())
     zipcode     2000-01-31     2000-02-29     2000-03-31     2000-04-30     2000-05-31  ...     2025-09-30     2025-10-31     2025-11-30     2025-12-31     2026-01-31     2026-02-28
29     75052  108084.074384  108192.712821  108295.498585  108517.027885  108778.370267  ...  323023.570642  322678.189125  322261.188479  321953.990884  321632.481692  321348.807761 
80     75217   48983.285582   49042.221489   49032.233448   49096.896381   49150.061681  ...  193889.566324  192391.420083  191858.797803  192097.031517  192716.796005  193431.490801 
152    75211   61372.723900   61421.095669   61407.840666   61532.187053   61822.557558  ...  226571.213688  225142.423302  223962.335327  223145.264651  223194.800982  223481.516211 
167    75228   74731.554367   74766.168054   74796.350793   74885.982566   75032.270676  ...  273745.934032  273633.376623  273985.508824  274567.042155  275082.450222  275059.668674 
283    75243   90444.072007   90405.125356   90311.894424   90292.237518   90359.300366  ...  296354.156436  296864.137238  297228.040485  297492.104605  297538.069655  297369.895579 
---
### print(sales_df.head())
 msa  2008-02-29  2008-03-31  2008-04-30  2008-05-31  2008-06-30  2008-07-31  ...  2025-07-31  2025-08-31  2025-09-30  2025-10-31  2025-11-30  2025-12-31  2026-01-31
4  Dallas, TX      4910.0      5578.0      6015.0      6711.0      6706.0      6601.0  ...      7155.0      6582.0      6043.0      6034.0      4730.0      5722.0      4212.0


---
### print(rent_df.head())
     zipcode   2015-01-31  2015-02-28   2015-03-31  2015-04-30   2015-05-31  ...   2025-08-31   2025-09-30   2025-10-31   2025-11-30   2025-12-31   2026-01-31
29     75052  1045.473143  1050.49227  1066.975219  1076.26448  1086.234684  ...  1680.607035  1669.159807  1657.390472  1648.701756  1642.998848  1644.000680
80     75217          NaN         NaN          NaN         NaN          NaN  ...  1643.247756  1637.771601  1692.281517  1740.900676  1806.364498  1808.750000
152    75211          NaN         NaN          NaN         NaN          NaN  ...  1590.961245  1599.616593  1593.876636  1587.728147  1567.958811  1553.902593
167    75228          NaN         NaN          NaN         NaN          NaN  ...  1218.272820  1164.759760  1130.054642  1118.604692  1148.280593  1167.052189
280    75243          NaN         NaN          NaN         NaN          NaN  ...  1062.498659  1069.881561  1062.179496  1059.597443  1047.523218  1025.515111

---
### print(listings_df.head())
          msa  2018-03-31  2018-04-30  2018-05-31  2018-06-30  2018-07-31  2018-08-31  ...  2025-07-31  2025-08-31  2025-09-30  2025-10-31  2025-11-30  2025-12-31  2026-01-31
4  Dallas, TX      8606.0     10075.0     11442.0     12067.0     12108.0     11538.0  ...     10557.0      9348.0      8406.0      7999.0      7165.0      6133.0      5632.0

---
### print(zillow_inventory_df.head())
          msa  2018-03-31  2018-04-30  2018-05-31  2018-06-30  2018-07-31  2018-08-31  ...  2025-07-31  2025-08-31  2025-09-30  2025-10-31  2025-11-30  2025-12-31  2026-01-31
4  Dallas, TX     24042.0     25876.0     28224.0     30490.0     32408.0     33567.0  ...     38954.0     39091.0     38265.0     37323.0     35777.0     33494.0     31270.0

---
### print(mortgage_rates_df.head())
                  Week  US30yrFRM 30yerFeesPoints  US15yrFRM  15yrFeesPoints  5/1ARM  5/1ARM_feesPoints  5/1ARM_margin  30yrFRM/5/1ARM_spread
0  1971-04-02 00:00:00       7.33             NaN        NaN             NaN     NaN                NaN            NaN                    NaN
1  1971-04-09 00:00:00       7.31                        NaN             NaN     NaN                NaN            NaN                    NaN
2  1971-04-16 00:00:00       7.31                        NaN             NaN     NaN                NaN            NaN                    NaN
3  1971-04-23 00:00:00       7.31                        NaN             NaN     NaN                NaN            NaN                    NaN
4  1971-04-30 00:00:00       7.29                        NaN             NaN     NaN                NaN            NaN                    NaN

---
### print(unemployment_rates_df.head())
   year  month  unemployment_rate
0  2024     12                3.7
1  2024     11                4.1
2  2024     10                4.1
3  2024      9                4.1
4  2024      8                4.4

---
### print(median_income_df[2018].head)
     ZCTA  median_income  total_population
0   75001          73094             15000
1   75006          61844             51136
2   75007          84541             54701
3   75019         121279             42597
4   75038          56636             30153
..    ...            ...               ...
90  75270     -666666666                 0
91  75287          51045             54882
92  75390     -666666666                 0
93  76051          85667             52454
94  76065          93071             35643

---
### print(school_rating_df[2018].head)
                                    District                                 Campus                  Region  County     School Type Score
2043  PEGASUS\nSCHOOL OF\nLIBERAL ARTS\nAND                                    NaN  REGION 10:\nRICHARDSON  DALLAS             NaN    86
2044  PEGASUS\nSCHOOL OF\nLIBERAL ARTS\nAND                   PEGASUS\nCHARTER H S  REGION 10:\nRICHARDSON  DALLAS  Elem/Secondary    86
2045                      UPLIFT\nEDUCATION                                    NaN  REGION 10:\nRICHARDSON  DALLAS             NaN    88
2046                      UPLIFT\nEDUCATION  UPLIFT\nEDUCATION\n-NORTH HILLS\nPREP  REGION 10:\nRICHARDSON  DALLAS     High School    94
2047                      UPLIFT\nEDUCATION  UPLIFT\nEDUCATION -\nUPLIFT\nGRAND PR  REGION 10:\nRICHARDSON  DALLAS      Elementary    68
...                                     ...                                    ...                     ...     ...             ...   ...
2846                            COPPELL ISD                        TOWN\nCENTER EL  REGION 10:\nRICHARDSON  DALLAS      Elementary    92
2847                            COPPELL ISD                  COTTONWOO\nD CREEK EL  REGION 10:\nRICHARDSON  DALLAS      Elementary    87
2848                            COPPELL ISD                       VALLEY\nRANCH EL  REGION 10:\nRICHARDSON  DALLAS      Elementary    97
2849                            COPPELL ISD                       DENTON\nCREEK EL  REGION 10:\nRICHARDSON  DALLAS      Elementary    94
2850                            COPPELL ISD                      RICHARD J\nLEE EL  REGION 10:\nRICHARDSON  DALLAS      Elementary    95

---
### print(crime_violent_df[\2018].head())
      agency month  offenses_per_100k  offenses  clearances  population
0  TX0570100    01              25.25       4.0         4.0     15839.0
1  TX0570100    02               0.00       0.0         1.0     15839.0
2  TX0570100    03              50.51       8.0         6.0     15839.0
3  TX0570100    04              50.51       8.0         5.0     15839.0
4  TX0570100    05              31.57       5.0         3.0     15839.0

### print(crime_property_df[\2018].head())
      agency month  offenses_per_100k  offenses  clearances  population
0  TX0570100    01             542.96      86.0        14.0     15839.0
1  TX0570100    02             359.87      57.0        11.0     15839.0
2  TX0570100    03             410.38      65.0        17.0     15839.0
3  TX0570100    04             410.38      65.0        19.0     15839.0
4  TX0570100    05             486.14      77.0        11.0     15839.0
