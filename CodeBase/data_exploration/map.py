import folium
from folium import Choropleth

# Function to create a Folium choropleth map
def create_map(geo_data, key_on, columns, fill_color = "YlOrRd", fill_opacity = 0.7,
               line_opacity = 0.7, legend_name = "", base_location = [40.7128, -74.0060],
               base_zoom_start = 11, tiles = "CartoDB Positron"):

    geo_data        = geo_data
    key_on          = key_on
    columns         = columns
    fill_color      = fill_color
    fill_opacity    = fill_opacity
    line_opacity    = line_opacity
    legend_name     = legend_name
    base_location   = base_location
    base_zoom_start = base_zoom_start
    tiles           = tiles

    # Create a new Folium map with the specified base location and zoom level
    folium_map       = folium.Map(location = base_location, zoom_start = base_zoom_start, tiles = tiles)

    # Add a Choropleth layer with the specified parameters
    Choropleth(
        geo_data     = geo_data.to_json(),
        data         = geo_data,
        columns      = columns,
        key_on       = key_on,
        fill_color   = fill_color,
        fill_opacity = fill_opacity,
        line_opacity = line_opacity,
        legend_name  = legend_name
    ).add_to(folium_map)

    return folium_map
