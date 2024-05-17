import os
import pandas as pd
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from Map import create_map
from Geographical_Data import to_geodataframe

## Displa full ouptut in pycharm console
pd.set_option('display.max_rows', None)            # Display all rows
pd.set_option('display.max_columns', None)         # Display all columns
pd.set_option('display.width', None)               # No truncation of content
pd.set_option('display.expand_frame_repr', False)  # Don't wrap DataFrame display

########## Read in the data

cwd = os.getcwd()

df                = cwd + f"\\Data\\Consolidated Monthly.csv"
demographics      = cwd + f"\\Data\\Demographics_2022.csv"
gdf               = cwd + f"\\Data\\GeoData.csv"

df           = pd.read_csv(df)  # df = pd.read_csv(cwd + f"\\Data\\Consolidated.csv")
df_daily     = pd.read_csv(cwd + f"\\Data\\full_data.csv")
demographics = pd.read_csv(demographics)
gdf          = pd.read_csv(gdf)
gdf          = to_geodataframe(gdf)

########## Data manipulation

df_daily          = df_daily.dropna(subset = ['Zip'])
df_daily          = df_daily.drop(columns = ['Dim1Name', 'Dim2Value'], axis = 1) # Drop the first column
df_daily['Date '] = pd.to_datetime(df_daily['Date '], dayfirst = True, errors = 'coerce')
df_daily['Date '] = pd.to_datetime(df_daily['Date '], format = "%d-%m-%y") # Convert the 'Date' column in ED to a proper date format
df_daily.columns  = ["Zip", "Age_Group", "Date",
                     "Count", "Anticipated_Cause"] # Rename the 6th column to "Age Group"
df['date']                 = pd.to_datetime(df['date'], format = "%b-%Y")
df['date']                 = pd.to_datetime(df['date'])
df['year']                 = df['date'].dt.year
df['month']                = df['date'].dt.month
demographics['ZIPCODE']    = demographics['ZIPCODE'].astype(int)
demographics['population'] = demographics['population'].astype(int)
df_july_2023               = df[(df['month'] == 7) & (df['year'] == 2023)]
df_july_2023               = df_july_2023.groupby('ZIPCODE').agg({
                                    'visits': 'sum',
                                    'max_lst': 'mean'
                                    }).reset_index()
df_july_2023['ZIPCODE']    = df_july_2023['ZIPCODE'].astype(int)

gdf.set_geometry('geometry', inplace = True)

df.head()
demographics.head()
gdf.head()

########## Exploratory Data Analysis

# # Function to set major ticks every 3 months and add a year label at appropriate positions
# def set_three_month_ticks(ax, df):
#     # Set ticks every 3 months (Jan, Apr, Jul, Oct)
#     ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))  # Jan, Apr, Jul, Oct
#     # Format ticks to show only the month
#     ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
#
#     # Add year labels for each unique year
#     unique_years = sorted(df['date'].dt.year.unique())  # Get unique years
#     for year in unique_years:
#         # Find the midpoint of each year and add the label above the axis
#         mid_date = df[df['date'].dt.year == year]['date'].median()
#         ax.text(mid_date, ax.get_ylim()[1], str(year), ha='center', va='bottom', fontsize=10)  # Add year label
#
#
# # Create a figure with two subplots
# fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))  # 2 rows, 1 column
#
# # Plot for 'visits'
# sns.lineplot(data=df, x='date', y='visits', ax=ax1, marker='o')
# ax1.set_title("Visits Over Time")
# ax1.set_xlabel("Date")
# ax1.set_ylabel("Number of Visits")
# ax1.grid(True)  # Enable grid lines for readability
# set_three_month_ticks(ax1, df)  # Set the desired x-axis ticks and labels
#
# # Plot for 'mean_lst'
# sns.lineplot(data=df, x='date', y='mean_lst', ax=ax2, marker='o')
# ax2.set_title("Mean LST Over Time")
# ax2.set_xlabel("Date")
# ax2.set_ylabel("Mean LST")
# ax2.grid(True)  # Enable grid lines
# set_three_month_ticks(ax2, df)  # Set the desired x-axis ticks and labels
#
# # Adjust layout to avoid overlap
# plt.tight_layout()  # Ensure proper spacing between subplots
#
# output_path = os.path.join(cwd, 'Figures', 'LST x ED Visits.png')
# #plt.savefig(output_path, dpi = 300)  # Save as PNG with high resolution (300 dpi)
#
# # Show the plots
# plt.show()
# plt.close(fig)

### Maps

# Convert 'date' to datetime

# Base map parameters for New York City

#### Map Visualization

base_location   = [40.7128, -74.0060]
base_zoom_start = 11
tile_type       = "CartoDB Positron" # Minimalistic tile type

# Create independent maps with the same initial parameters

lst_map    = create_map(geo_data = gdf, key_on = "feature.properties.ZIPCODE", columns = ["ZIPCODE", "max_lst"],
                        fill_color = 'YlOrRd', fill_opacity = 0.7, line_opacity = 0.7, legend_name = "July Max LST 2023",
                        base_location = base_location, base_zoom_start = base_zoom_start, tiles = tile_type)
visits_map = create_map(geo_data = gdf, key_on = "feature.properties.ZIPCODE", columns = ["ZIPCODE", "visits"],
                        fill_color = 'YlOrRd', fill_opacity = 0.7, line_opacity = 0.7, legend_name = "July Visits 2023",
                        base_location = base_location, base_zoom_start = base_zoom_start, tiles = tile_type)
pop_map    = create_map(geo_data = gdf, key_on = "feature.properties.ZIPCODE", columns = ["ZIPCODE", "population"],
                        fill_color = 'YlOrRd', fill_opacity = 0.7, line_opacity = 0.7, legend_name = "Population per Zip",
                        base_location = base_location, base_zoom_start = base_zoom_start, tiles = tile_type)
dist_map   = create_map(geo_data = gdf, key_on = "feature.properties.ZIPCODE", columns = ["ZIPCODE", "dist"],
                        fill_color = 'YlOrRd', fill_opacity = 0.7, line_opacity = 0.7, legend_name = "ER Visits per Population",
                        base_location = base_location, base_zoom_start = base_zoom_start, tiles = tile_type)

# Save the maps to HTML
lst_map.save(cwd + "\\Figures\\lst_map.html")
visits_map.save(cwd + f"\\Figures\\visits_map.html")
pop_map.save(cwd + f"\\Figures\\pop_map.html")
dist_map.save(cwd + f"\\Figures\\dist_map.html")

correlation = df['visits'].corr(df['max_lst'])
print(correlation)

#### Daily Data per zip

num_zips = 3
start_date = datetime(year = 2023, month = 7, day = 1)
end_date   = datetime(year = 2023, month = 7, day = 31)

# Step 1: Read and clean data
df_daily = pl.read_csv(cwd + f"\\Data\\full_data.csv")  # Replace with your actual path

# Drop null Zip codes, remove specified columns, and convert Date
df_daily = df_daily.drop_nulls("Zip")
df_daily = df_daily.drop(["Dim1Name", "Dim2Value"])  # Dropping unwanted columns
df_daily = df_daily.with_columns(
    # pl.col("Date ")  # note the extra space
    # .str.to_date("%m-%d-%y", strict = False)
    # .alias("Date "),
    (pl.col("Zip").cast(pl.Utf8))
)
df_daily = df_daily.rename({"Date ": "Date"})  # Correcting the column name
df_daily = df_daily.with_columns(pl.col("Zip").cast(pl.Utf8))

# Step 2: Get top 10 zip codes by visits
top_10_zipcodes = (
    df[df["date"] == "Jul-2023"]
    .sort_values(by = "visits", ascending = False)
    .head(num_zips)
    ["ZIPCODE"]
)
top_10_zipcodes_str = []
for zipcode in top_10_zipcodes:
    # Convert each element to a string
    zipcode_str = str(zipcode)
    top_10_zipcodes_str.append(zipcode_str)

# Step 3: Filter data for top 10 zip codes and compute daily mean
df_daily = df_daily.filter(pl.col("Zip").is_in(top_10_zipcodes_str))

def swap_date_parts(date_str):
    # Check if the date has '/'
    if '/' in date_str:
        # Split the date string by '/', swap the first and last parts
        parts = date_str.split('/')
        # Swap the first and last parts
        if len(parts[0]) <= 1:
            swapped_date = f"{parts[1]}-0{parts[0]}-{parts[2]}"
        else:
            swapped_date = f"{parts[1]}-{parts[0]}-{parts[2]}"
        return swapped_date
    else:
        return date_str

df_daily = df_daily.with_columns(
    df_daily['Date'].map_elements(swap_date_parts, return_dtype=pl.Utf8).alias("Date")
)

df_daily = df_daily.with_columns(
    pl.col("Date")  # note the extra space
    .str.to_date("%d-%m-%y", strict = False)
    .alias("Date"),
)
df_daily = df_daily.filter(
    (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
)
df_daily = df_daily.groupby(["Zip", "Date"]).agg(
                            pl.sum("Count").alias("Count")  # Sum of 'Count' for each group
                        )
df_daily.head(13)


df_mean = (
    df_daily.groupby("Date")
    .agg(pl.mean("Count").alias("Mean"))
)
df_mean.head(13)

# Merge with filtered data to get deviation from daily mean
df_deviation = (
    df_daily
    .join(df_mean, on = "Date")
    .with_columns((pl.col("Count") - pl.col("Mean")).alias("Deviation"))
)
df_deviation.head(13)

### Plotting
fig, axs = plt.subplots(2, 1, figsize = (12, 12))

# Plot daily trend for top zip codes
sns.lineplot(data = df_daily.to_pandas(), x = "Date", y = "Count", ci = None, hue = "Zip", ax = axs[0])
axs[0].set_title("Daily Trend of ER Visits for Top Zip Codes")
axs[0].set_xlabel("Date")
axs[0].set_ylabel("Visits")
axs[0].set_ylim(0, None)
axs[0].tick_params(axis = 'x', rotation = 45)

# Plot deviation from daily mean for the same zip codes
sns.lineplot(data = df_deviation.to_pandas(), x = "Date", y = "Deviation", ci = None, hue = "Zip", ax = axs[1])
axs[1].set_title("Deviation from Daily Mean for Top Zip Codes")
axs[1].set_xlabel("Date")
axs[1].set_ylabel("Deviation")
axs[1].tick_params(axis = 'x', rotation = 45)

plt.tight_layout(pad = 4)
# fig.subplots_adjust(hspace = 1)
plt.show()
plt.save(cwd + f"\\Figures\\summer_er.png")

print(df_daily.groupby(["Zip"]).agg(
          pl.sum("Count").alias("Count")  # Sum of 'Count' for each group
      ))


X = df[['max_lst']]  # Features
y = df['visits']  # Target variable

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=7)

# Create and fit the linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Make predictions on the test set
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)  # Mean Squared Error
r2 = r2_score(y_test, y_pred)  # R-squared score

print("Mean Squared Error:", mse)
print("R-squared Score:", r2)