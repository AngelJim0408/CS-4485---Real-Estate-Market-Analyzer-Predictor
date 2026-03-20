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
### print(zhvi_dallas_county_df.head())
     RegionID  SizeRank  RegionName RegionType StateName State  ...     2025-09-30     2025-10-31     2025-11-30     2025-12-31     2026-01-31     2026-02-28
29      90654        30       75052        zip        TX    TX  ...  323023.570642  322678.189125  322261.188479  321953.990884  321632.481692  321348.807761
80      90769        82       75217        zip        TX    TX  ...  193889.566324  192391.420083  191858.797803  192097.031517  192716.796005  193431.490801
152     90764       158       75211        zip        TX    TX  ...  226571.213688  225142.423302  223962.335327  223145.264651  223194.800982  223481.516211
167     90780       173       75228        zip        TX    TX  ...  273745.934032  273633.376623  273985.508824  274567.042155  275082.450222  275059.668674
283     90795       290       75243        zip        TX    TX  ...  296354.156436  296864.137238  297228.040485  297492.104605  297538.069655  297369.895579

---
### print(zillow_sales_df.head())
   RegionID  SizeRank       RegionName RegionType StateName  2008-02-29  2008-03-31  ...  2025-07-31  2025-08-31  2025-09-30  2025-10-31  2025-11-30  2025-12-31  2026-01-31
0    102001         0    United States    country       NaN    203865.0    236010.0  ...    357093.0    341242.0    326500.0    331480.0    262834.0    300866.0    215688.0
1    394913         1     New York, NY        msa        NY      8527.0      9057.0  ...     14834.0     14362.0     13783.0     13625.0     11104.0     13230.0     10297.0
2    753899         2  Los Angeles, CA        msa        CA      4134.0      5048.0  ...      6981.0      6498.0      6639.0      7001.0      5389.0      6127.0      4437.0
3    394463         3      Chicago, IL        msa        IL      5604.0      6960.0  ...     10148.0      9269.0      8545.0      8771.0      6585.0      7506.0      5090.0
4    394514         4       Dallas, TX        msa        TX      4910.0      5578.0  ...      7155.0      6582.0      6043.0      6034.0      4730.0      5722.0      4212.0

---
### print(zillow_rent_df.head())
   RegionID  SizeRank  RegionName RegionType StateName State      City  ...   2025-07-31   2025-08-31   2025-09-30   2025-10-31   2025-11-30   2025-12-31   2026-01-31
0     91982         1       77494        zip        TX    TX      Katy  ...  1849.986968  1855.353309  1849.102029  1839.240212  1826.238505  1822.140686  1803.813488
1     61148         2        8701        zip        NJ    NJ  Lakewood  ...          NaN          NaN          NaN          NaN          NaN          NaN  2385.000000
2     91940         3       77449        zip        TX    TX      Katy  ...  1828.631792  1819.567524  1810.683358  1809.883944  1799.512657  1810.114483  1800.547424
3     62080         4       11368        zip        NY    NY  New York  ...          NaN          NaN          NaN          NaN          NaN  2185.628181  2304.750000
4     91733         5       77084        zip        TX    TX   Houston  ...  1602.443131  1609.637147  1608.350155  1602.471577  1590.745289  1584.603251  1585.098746

---
### print(zillow_listings_df.head())
   RegionID  SizeRank       RegionName RegionType StateName  2018-03-31  2018-04-30  ...  2025-07-31  2025-08-31  2025-09-30  2025-10-31  2025-11-30  2025-12-31  2026-01-31
0    102001         0    United States    country       NaN    391819.0    453879.0  ...    402901.0    372085.0    355290.0    344387.0    308410.0    252592.0    228268.0
1    394913         1     New York, NY        msa        NY     20008.0     23619.0  ...     14733.0     12896.0     12638.0     12392.0     11416.0      8688.0      7670.0
2    753899         2  Los Angeles, CA        msa        CA      9187.0     10007.0  ...      8315.0      7656.0      7226.0      6815.0      5989.0      4845.0      4802.0
3    394463         3      Chicago, IL        msa        IL     12675.0     15359.0  ...     10455.0      9627.0      9110.0      8641.0      7449.0      5688.0      4727.0
4    394514         4       Dallas, TX        msa        TX      8606.0     10075.0  ...     10557.0      9348.0      8406.0      7999.0      7165.0      6133.0      5632.0

---
### print(zillow_inventory_df.head())
   RegionID  SizeRank       RegionName RegionType StateName  2018-03-31  2018-04-30  ...  2025-07-31  2025-08-31  2025-09-30  2025-10-31  2025-11-30  2025-12-31  2026-01-31
0    102001         0    United States    country       NaN   1421529.0   1500195.0  ...   1373264.0   1380205.0   1373116.0   1362806.0   1322706.0   1239512.0   1157399.0
1    394913         1     New York, NY        msa        NY     73707.0     80345.0  ...     49038.0     48097.0     47265.0     46782.0     45632.0     41945.0     38066.0
2    753899         2  Los Angeles, CA        msa        CA     21998.0     23784.0  ...     26379.0     26688.0     26539.0     25801.0     24240.0     21853.0     20023.0
3    394463         3      Chicago, IL        msa        IL     38581.0     42253.0  ...     24026.0     24129.0     24076.0     23989.0     22985.0     20628.0     18331.0
4    394514         4       Dallas, TX        msa        TX     24042.0     25876.0  ...     38954.0     39091.0     38265.0     37323.0     35777.0     33494.0     31270.0

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
### print(median_income_df[\2018].head)
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
### print(school_rating_df[\2018].head)
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
