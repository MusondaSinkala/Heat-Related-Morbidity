### sum up the total visits per zip over days that have max_lst above 35 and three days afterwards.
### if two consecutive days have max_lst above 35 then sum over the following 4 days,
### if three consecutive days have max_lst above 35 then sum over the following 5 days.
### The result should be a data frame of the following form:
### date_of_event   =   first date of max_lst above 35
### zip   =   the zip code in question
### duration_of_event   =   number of days summed over
### number of visits   =   sum of visit column
### for context, this would be a dataframe containing the dates of the various heatwave across various zip codes,
### the duration of the heatwave and the number of emergency department visits during that heat wave
### then create a separate dataframe which would just be the original dataframe minus the dates and
### associated information that have been captured in the dataframe detailed above

import os
import polars as pl

# Display full output in PyCharm console
pl.Config.set_tbl_cols(40)  # Display up to 40 columns
pl.Config.set_tbl_rows(40)  # Display up to 40 rows

# Read in the data
cwd = os.getcwd()
file_path = os.path.join(cwd, "Data", "Daily Data", "sample.csv")
df = pl.read_csv(file_path)

# Convert 'date' column to datetime and 'max_lst' to float
df = df.with_columns([
    pl.col('date').str.strptime(pl.Date, format="%Y-%m-%d"),
    pl.col('max_lst').cast(pl.Float64)
])

# Function to calculate total visits per zip over heatwave periods
def calculate_heatwave_visits(df):
    heatwave_events = []

    # Add a column to indicate heatwave days
    df = df.with_columns(
        (pl.col("max_lst") > 38).alias("is_heatwave")
    )

    # Sort the dataframe by date to ensure consecutive day calculation
    df = df.sort("date")

    i = 0
    while i < len(df):
        if df[i, 'is_heatwave']:
            start_date = df[i, 'date']
            zip_code = df[i, 'zip']
            duration = 4
            end_date = start_date + pl.duration(days = 3)

            # Check for consecutive heatwave days
            j = 1
            while i + j < len(df) and df[i + j, 'is_heatwave']:
                duration += 1
                end_date = start_date + pl.duration(days=j + 3)
                j += 1

            visits_sum = df.filter(
                (pl.col("date") >= start_date) &
                (pl.col("date") <= end_date) &
                (pl.col("zip") == zip_code)
            )['visits'].sum()

            heatwave_events.append({
                'date_of_event': start_date,
                'zip': zip_code,
                'duration_of_event': duration,
                'number_of_visits': visits_sum
            })

            # Skip the next consecutive heatwave days
            i += j
        else:
            i += 1

    return pl.DataFrame(heatwave_events)

# Calculate heatwave visits
heatwave_visits_df = calculate_heatwave_visits(df)

# Create separate dataframe without dates captured in heatwave_visits_df
captured_dates = heatwave_visits_df['date_of_event'].unique()
filtered_df = df.filter(~pl.col('date').is_in(captured_dates))

print("Heatwave Visits DataFrame:")
print(heatwave_visits_df)

print("\nFiltered DataFrame:")
print(filtered_df)



#####################################################################################################################

import geopandas as gpd
import matplotlib.pyplot as plt
import requests
import pyproj
import datetime as dt
import xarray as xr
import rioxarray
import shapely
import rasterio
from flatten_polygon import flatten_multipolygon

print("This Notebook was produced with the following versions")
print("geopanda version   : ", gpd.__version__)
print("pyproj version     : ", pyproj.__version__)
print("rioxarray version  : ", rioxarray.__version__)
print("xarray version     : ", xr.__version__)
print("shapely            : ", shapely.__version__)
print("rasterio           : ", rasterio.__version__)

grsm_poly = gpd.read_file(cwd + f"\\Shapefiles\\tl_2020_us_zcta520.shp")
nyc_zip_codes     = cwd + f"\\Shapefiles\\nyc_zip_codes.txt"
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
with open(nyc_zip_codes, "r") as file:
    zip_codes = [zip_code.strip() for line in file for zip_code in line.split(",")]
zip_codes = [int(zip_code) for zip_code in zip_codes if zip_code]
nyc_zip_gdf = grsm_poly[grsm_poly['ZCTA5CE20'].astype(int).isin(zip_codes)] # filter for relevant zip codes
nyc_zip_gdf['ZCTA5CE20'] = nyc_zip_gdf['ZCTA5CE20'].astype(int)
nyc_zip_gdf['ZCTA5CE20'] = nyc_zip_gdf['ZCTA5CE20'].map(zip_mapping).fillna(nyc_zip_gdf['ZCTA5CE20'])
nyc_zip_gdf['geometry'] = nyc_zip_gdf['geometry'].apply(flatten_multipolygon)
nyc_zip_gdf.set_geometry('geometry', inplace = True)

ax = nyc_zip_gdf.plot() # boundary file of the Great Smokey Mountains National Park

nyc_zip_gdf.crs  # Lists the coordinate reference system (crs)

### Step 1.1

# Let's determine and store the geographic bounding box of the Park boundary for Earthdata Searching
# data can be re-projected using the GeoDataFrame.to_crs() command:
grsm_poly_4326 = nyc_zip_gdf.to_crs(epsg = 4326) #EPSG code for WGS84
xy = grsm_poly_4326.bounds # bound of polygon in lat, lon
print(xy)

xy = grsm_poly_4326.bounds.values.tolist()[0] # We'll need the bounding box as a Python list
                                              # to server as a subsetting parameter
print(xy)

### Step 1.2

#defining Daymet proj - we'll use this in a later step
daymet_proj = "+proj=lcc +ellps=WGS84 +a=6378137 +b=6356752.314245 +lat_1=25 +lat_2=60 +lon_0=-100 +lat_0=42.5 +x_0=0 +y_0=0 +units=m +no_defs"
grsm_poly_lcc = nyc_zip_gdf.to_crs(daymet_proj) # to_crs re-projects from UTM 17N to LCC
lccbounds = nyc_zip_gdf.bounds # Bounds in LCC projection
lccbounds.round(2)

fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(12, 8))
#grsm_poly.plot(ax=ax1, facecolor='blue');
grsm_poly_4326.plot(ax=ax1, facecolor='blue');
ax1.set_title("GRSM Geographic Coordinate System");
grsm_poly_lcc.plot(ax=ax2, facecolor='grey');
ax2.set_title("GRSM Daymet LCC Projection");
plt.tight_layout()

### Step 1.3

start_date = dt.datetime(2023, 5, 1) # specify your own start date
end_date = dt.datetime(2023, 8, 31)  # specify your end start date

dt_format = '%Y-%m-%dT%H:%M:%SZ' # format requirement for datetime search
temporal_str = start_date.strftime(dt_format) + ',' + end_date.strftime(dt_format)

var = 'prcp' # select a Daymet variable of interest
print(temporal_str)
print(var)

### Step 2.1

daymet_doi = '10.3334/ORNLDAAC/1840' # define the Daymet V4 Daily Data DOI as the variable `daymet_doi`
cmrurl='https://cmr.earthdata.nasa.gov/search/' # define the base url of NASA's CMR API as the variable `cmrurl`
doisearch = cmrurl + 'collections.json?doi=' + daymet_doi # Create the Earthdata Collections URL
print('Earthdata Collections URL: Daymet V4 Daily -->', doisearch)

# From the doisearch, we can obtain the ConceptID for the Daymet V4 Daily data
# We'll search the json response of the Daymet metadata for "id" within the 'entry' dictionary key
response = requests.get(doisearch)
collection = response.json()['feed']['entry'][0]
#print(collection)
concept_id = collection['id']
print('NASA Earthdata Concept_ID --> ' , concept_id)