########## Import relevant packages
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import sqlite3
from shapely.ops import unary_union
from flatten_polygon import flatten_multipolygon

## Display full ouptut in pycharm console
pd.set_option('display.max_rows', None)            # Display all rows
pd.set_option('display.max_columns', None)         # Display all columns
pd.set_option('display.width', None)               # No truncation of content
pd.set_option('display.expand_frame_repr', False)  # Don't wrap DataFrame display

########## Read in the data

cwd = os.getcwd()

demographics      = cwd + f"\\Data\\Demographics_2022.csv"
nyc_zip_shapefile = cwd + f"\\Shapefiles\\tl_2020_us_zcta520.shp"
nyc_zip_codes     = cwd + f"\\Shapefiles\\nyc_zip_codes.txt"
ED                = cwd + f"\\Data\\Daily Data\\ER visits.csv"
LST               = cwd + f"\\Data\\Daily Data\\Raw Data\\Land Surface Temperature.csv"
zip_mapping       = {
    10001: 10036, 10003: 10003, 10005: 10038, 10006: 10038, 10007: 10038,
    10012: 10003, 10017: 10016, 10018: 10036,
    10020: 10019,
    10036: 10036, 10037: 10030, 10038: 10038, 10039: 10030,
    10044: 10128,
    10065: 10021, 10069: 10023,
    10075: 10162,
    10103: 10019, 10110: 10036, 10111: 10019, 10112: 10019, 10119: 10036, 10152: 10022, 10153: 10022, 10154: 10022, 10165: 10016, 10167: 10016, 10168: 10016, 10169: 10016, 10170: 10016, 10171: 10016, 10172: 10016, 10173: 10016, 10174: 10016, 10177: 10016, 10199: 10036,
    10271: 10038, 10278: 10038, 10279: 10038, 10280: 10038, 10282: 10038,
    10302: 10302, 10303: 10302, 10307: 10309, 10308: 10312, 10310: 10302, 10312: 10312,
    10454: 10454, 10463: 10463, 10470: 10466, 10471: 10463, 10474: 10454,
    11104: 11101, 11109: 11101,
    11208: 11208, 11232: 11220, 11239: 11236,
    11356: 11357, 11359: 11357, 11360: 11357, 11362: 11361, 11363: 11361, 11366: 11365, 11371: 11369,
    11411: 11413, 11414: 11417, 11415: 11418, 11416: 11418, 11423: 11432, 11428: 11427, 11429: 11413, 11430: 11434, 11434: 11434, 11436: 11434, 11451: 11433,
    11691: 11691, 11692: 11691, 11693: 11693, 11694: 11693, 11697: 11693
}

# ED = pd.read_csv(cwd + f"\\Data\\full_data.csv")
ED                = pd.read_csv(ED)
LST               = pd.read_csv(LST)
demographics      = pd.read_csv(demographics)
# nyc_zip_gdf       = gpd.read_file(nyc_zip_shapefile) # Load the shapefile
# Load the NYC zip codes
with open(nyc_zip_codes, "r") as file:
    zip_codes = [zip_code.strip() for line in file for zip_code in line.split(",")]
zip_codes = [int(zip_code) for zip_code in zip_codes if zip_code]

########## Data Manipulation

#### Non-Map Data Manipulation

# ED = ED.dropna(subset = ['Zip'])
# ED = ED.drop(ED.columns[:1], axis = 1) # Drop the first three columns
# ED['Date'] = pd.to_datetime(ED['Date'], dayfirst = True, errors = 'coerce')
# ED['Date'] = pd.to_datetime(ED['Date'], format = "%d-%m-%y") # Convert the 'Date' column in ED to a proper date format
# ED.columns = ["Zip", "Dim2Value", "Age_Group",
#               "Date", "Count", "Anticipated_Cause"] # Rename the 6th column to "Age Group"

ED          = ED.drop(ED.columns[:3], axis = 1) # Drop the first three columns
ED          = ED.drop("Dim2Value", axis = 1) # Drop the 2nd column
ED['Date']  = pd.to_datetime(ED['Date'], format="%b-%y") # Convert the 'Date' column in ED to a proper date format
# ED['Date']  = pd.to_datetime(ED['Date'])
# ED['Day']   = ED['Date'].dt.day
ED['Month'] = ED['Date'].dt.month  # Extract the month
ED['Year']  = ED['Date'].dt.year  # Extract the year

# Create a new 'date' column in LST by concatenating the 'year' and 'month' columns
LST['Date'] = pd.to_datetime(LST[['year', 'month']].assign(day = 1))  # Adding a day value for complete date
LST.columns = ["Zip", "Year", "Month",
               "Max_LST", "Date"] # Rename zip code
LST['year'] = LST['year'].astype(int)
LST['month'] = LST['month'].astype(int)
LST['day'] = LST['day'].astype(int)

ED.head()
LST.head()

# Connect to an SQLite database in memory
connection = sqlite3.connect(":memory:")

# Save dataframes to the SQLite database
ED.to_sql("ED", connection, index = False, if_exists = "replace")
LST.to_sql("LST", connection, index = False, if_exists = "replace")

# SQL query to join ED and LST on date and zip code
sql_query = """ select e.Date as date,
                       cast(e.Zip as int) as zip,
                       sum(e.Count) as visits,
                       max(l.meanLST) as max_lst
                from ED e
                left join LST l
                       on e.Year = l.year
                      and e.Month = l.month
                      and e.Day = l.day
                      and cast(e.Zip as int) = l.zip_code
                where e.Year = 2023
                  and e.Month in (5, 6, 7, 8)
                group by 1, 2
            """
# sql_query = """ select l.Date as date,
#                        e.Zip as ZIPCODE,
#                        sum(e.Count) as visits,
#                        max(l.Max_LST) as max_lst
#                 from ED e
#                 inner join LST l
#                         on e.Month = l.Month
#                        and e.Year = l.Year
#                        and e.Zip = l.Zip
#                 group by 1, 2
#                 order by 1, 2
#             """
# Execute the SQL query
df = pd.read_sql_query(sql_query, connection)
df['date'] = pd.to_datetime(df['date'])
df['date'] = df['date'].dt.strftime('%b-%Y')

# Apply the mapping to the 'zip' column
df['ZIPCODE'] = df['ZIPCODE'].map(zip_mapping).fillna(df['ZIPCODE'])

#### Map Data Manipulation

cons                         = df
cons['date']                 = pd.to_datetime(cons['date'], format = "%b-%Y")
cons['date']                 = pd.to_datetime(cons['date'])
cons['year']                 = cons['date'].dt.year
cons['month']                = cons['date'].dt.month
demographics['ZIPCODE']      = demographics['ZIPCODE'].astype(int)
demographics['population']   = demographics['population'].astype(int)
df_july_2023                 = cons[(cons['month'] == 7) & (cons['year'] == 2023)]
df_july_2023                 = df_july_2023.groupby('ZIPCODE').agg({
                                    'visits': 'sum',
                                    'max_lst': 'mean'
                                    }).reset_index()
df_july_2023['ZIPCODE']      = df_july_2023['ZIPCODE'].astype(int)

# nyc_zip_gdf_full = nyc_zip_gdf
# nyc_zip_gdf = nyc_zip_gdf_full
nyc_zip_gdf              = nyc_zip_gdf[nyc_zip_gdf['ZCTA5CE20'].astype(int).isin(zip_codes)] # filter for relevant zip codes
nyc_zip_gdf              = nyc_zip_gdf[['ZCTA5CE20', 'GEOID20', 'geometry']]
nyc_zip_gdf['ZCTA5CE20'] = nyc_zip_gdf['ZCTA5CE20'].astype(int)
nyc_zip_gdf['ZIPCODE']   = nyc_zip_gdf['ZCTA5CE20'].map(zip_mapping).fillna(nyc_zip_gdf['ZCTA5CE20']) # Apply the relevant zipcode mapping
nyc_zip_gdf              = nyc_zip_gdf.set_index("ZCTA5CE20")
nyc_zip_gdf.index        = nyc_zip_gdf.index.astype(int)

# merge the 3 datasets
nyc_zip_gdf              = nyc_zip_gdf.merge(df_july_2023, on = 'ZIPCODE', how = 'left')
nyc_zip_gdf              = nyc_zip_gdf.merge(demographics, on = 'ZIPCODE', how = 'left')
nyc_zip_gdf['dist']      = round((nyc_zip_gdf['visits'] / nyc_zip_gdf['population']) * 100, 1)
if nyc_zip_gdf['max_lst'].isnull().sum() > 0:
    nyc_zip_gdf.dropna(subset = ['max_lst'], inplace = True)
nyc_zip_gdf              = nyc_zip_gdf.groupby('ZIPCODE', as_index = False).agg({
                           'geometry': lambda geom: unary_union(geom),
                           'visits': 'mean',
                           'max_lst': 'mean',
                           'population': 'sum',
                           'dist': 'mean'
                           })
nyc_zip_gdf['geometry'] = nyc_zip_gdf['geometry'].apply(flatten_multipolygon)
nyc_zip_gdf.set_geometry('geometry', inplace = True)

nyc_zip_gdf.head()
df.head()

# df.to_csv(cwd + f"\\Data\\Consolidated.csv", index = False)
df.to_csv(cwd + f"\\Data\\Consolidated Monthly.csv", index = False)
nyc_zip_gdf.to_csv(cwd + f"\\Data\\GeoData.csv", index = False)


#########################################################################################

filtered_ED = ED[(ED['Zip'] != 'Citwide') & (ED['Year'] == 2023) & (ED['Month'].isin([5, 6, 7, 8]))]
filtered_ED['Zip'] = filtered_ED['Zip'].astype(str).str.split('.').str[0].astype(int)
filtered_ED = filtered_ED[(filtered_ED['Age_Group'] == 'All age groups') & (ED['Year'] == 2023) & (ED['Month'].isin([5, 6, 7, 8]))]
test = filtered_ED.groupby(['Date', 'Zip', 'Year', 'Month', 'Day']).agg({'Count':'sum'}).reset_index()
merged_df = test.merge(LST,
                      left_on =  ['Year', 'Month', 'Day', 'Zip'],
                      right_on = ['year', 'month', 'day', 'zip_code'],
                      how = 'left')
merged_df = merged_df.drop(columns = ['zip_code', 'year', 'month', 'day', 'Date_y'])
merged_df.rename(columns = {'Date_x':'date', 'Zip':'zip', 'Year': 'year', 'Month': 'month',
                            'Day':'day', 'Count':'visits', 'meanLST':'max_lst'}, inplace = True)
merged_df[merged_df['maxLST'] > 0].head(50)
merged_df.to_csv(cwd + f"\\Data\\Daily Data\\sample.csv", index = False)
