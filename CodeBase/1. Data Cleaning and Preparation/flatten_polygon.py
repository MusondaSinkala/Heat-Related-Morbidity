from shapely.geometry import MultiPolygon, Polygon

# Function to flatten a MultiPolygon into a single Polygon
def flatten_multipolygon(multi_polygon):
    if isinstance(multi_polygon, MultiPolygon):
        # Get all coordinates from the exteriors of the polygons
        coordinates = [list(p.exterior.coords) for p in multi_polygon.geoms]

        # Flatten the list of coordinates
        flattened_coordinates = [coord for sublist in coordinates for coord in sublist]

        # Create a single Polygon from the flattened coordinates
        return Polygon(flattened_coordinates)
    return multi_polygon