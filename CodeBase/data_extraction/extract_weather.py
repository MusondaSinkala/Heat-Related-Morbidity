########## Import relevant packages
import os
import polars as pl
import ee

cwd = os.getcwd()

## Display full ouptut in pycharm console
pl.Config.set_tbl_cols(40)  # Display up to 40 columns
pl.Config.set_tbl_rows(40)  # Display up to 40 rows

########## Connect to Earth Engine
ee.Authenticate() # Authenticate to Earth Engine
json_file       = cwd + f"/Keys/ee-musondaksinkala-6d598041af3c.json"
service_account = 'musonda-sinkala@ee-musondaksinkala.iam.gserviceaccount.com'
credentials     = ee.ServiceAccountCredentials(service_account, json_file)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file
ee.Initialize(credentials) # Initialize Earth Engine

########## Extract Data

start   = '2023-05-01'
end     = '2023-08-31'
dataset = ee.ImageCollection('NASA/ORNL/DAYMET_V4').filterDate(start, end)

variables = dataset.select(['tmax', 'prcp', 'dayl', 'vp'])

# Define the area of interest - NYC
nyc_aoi = ee.Geometry.Rectangle([-74.25559, 40.49612, -73.70001, 40.91553])

# Import zip code boundaries
nyc_zip_codes = ee.FeatureCollection('TIGER/2010/ZCTA5') \
    .filterBounds(nyc_aoi)  # Ensure it is within NYC boundaries

# Define the scale in meters
scale = 10000  # Scale in meters

# List to store selected properties for all dates
selected_properties = []

# Define the date range
start_date = ee.Date(start)
end_date   = ee.Date(end)
date_range = ee.List.sequence(start_date.millis(), end_date.millis(), 24 * 60 * 60 * 1000)

# Loop through each day in the date range
for date_millis in date_range.getInfo():
    date = ee.Date(date_millis)

    # Filter the image collection for the specific date
    variables_day = variables.filterDate(date, date.advance(1, 'day'))

    # Reduce the image collection to a single image using maximum reduction
    max_variables = variables_day.max().clip(nyc_aoi)

    # Sample the image within the AOI and reduce by median
    samples = max_variables.reduceRegions(collection=nyc_zip_codes, reducer=ee.Reducer.median(), scale=scale)

    # Get the results for the specific date
    result = samples.getInfo()

    # Append the results to the selected_properties list
    for feature in result['features']:
        properties = feature['properties']
        # Select desired properties
        selected = {
            'date': date.format('YYYY-MM-dd').getInfo(),  # Add date property
            'GEOID10': properties['GEOID10'],
            'ZCTA5CE10': properties['ZCTA5CE10'],
            'dayl': properties['dayl'],
            'prcp': properties['prcp'],
            'tmax': properties['tmax'],
            'vp': properties['vp']
        }
        selected_properties.append(selected)

df = pl.DataFrame(selected_properties)
print(df.head(10))

df.write_csv(cwd + '\\Data\\Daily Data\\Daily Weather Data.csv')
