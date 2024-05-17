import ee

# Authenticate to Earth Engine
ee.Authenticate()

# Initialize Earth Engine
ee.Initialize()


# Define a region of interest (e.g., New York City)
nyc_geometry = ee.Geometry.Rectangle([-74.2591, 40.4774, -73.7002, 40.9176])

# Load an image collection (e.g., Landsat 8)
collection = ee.ImageCollection('LANDSAT/LC08/C01/T1_TOA') \
    .filterBounds(nyc_geometry) \
    .filterDate('2020-01-01', '2020-12-31') \
    .sort('CLOUD_COVER')

# Select an image
image = collection.first()

# Calculate NDVI
ndvi = image.normalizedDifference(['B5', 'B4'])

# Print NDVI
print("NDVI:", ndvi.getInfo())
