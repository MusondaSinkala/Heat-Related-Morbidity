import geopandas as gpd
from shapely import wkt

def to_geodataframe(df, geometry = 'geometry', crs= 'EPSG:4326'):
    """
    Converts a DataFrame with a geometry column into a GeoDataFrame for choropleth mapping.

    Parameters:
    - df (pandas.DataFrame): The input DataFrame with a geometry column.
    - geometry_column (str): The name of the geometry column in the DataFrame (default is 'geometry').
    - crs (str): The default Coordinate Reference System (default is 'EPSG:4326').

    Returns:
    - GeoDataFrame: The converted GeoDataFrame with valid geometries and specified CRS.
    """

    # Ensure the geometry column has shapely geometries
    def validate_geometry(g):
        if isinstance(g, str):
            try:
                return wkt.loads(g)
            except Exception as e:
                print(f"Error converting geometry: {e}, skipping this geometry.")
                return None
        return g  # if it's already a valid geometry, return as-is

    # Apply the validation function to convert to shapely geometries
    df[geometry] = df[geometry].apply(validate_geometry)

    # Drop rows with invalid geometries
    df = df.dropna(subset = [geometry])

    # Create the GeoDataFrame with the specified CRS
    gdf = gpd.GeoDataFrame(df, geometry = geometry)
    gdf.set_crs(crs, inplace = True)

    return gdf
