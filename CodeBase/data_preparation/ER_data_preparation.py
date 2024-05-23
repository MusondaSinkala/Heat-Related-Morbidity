import os
import polars as pl
import re
import time

## Display full ouptut in pycharm console
pl.Config.set_tbl_cols(40)  # Display up to 40 columns
pl.Config.set_tbl_rows(40)  # Display up to 40 rows

cwd = os.getcwd()  # Get the current working directory

# File names for the Excel files
file_names = ["Asthma.xlsx", "Respiratory.xlsx", "Diarrhea.xlsx", "Influenza.xlsx", "Vomitting.xlsx"]

# List to hold DataFrames for each file
dfs = []

# Loop through each file name, read the data, and append to the list
for file in file_names:
    df = pl.read_excel(cwd + f"\\Data\\Archive\\1. Daily Data\\" + file)  # Read the Excel file with Polars
    df = df.select(df.columns[2:])  # Drop the first two columns
    dfs.append(df)

# Concatenate all DataFrames into one
ED = pl.concat(dfs)  # Concatenate without re-indexing manually
ED = ED.filter(pl.col("Zip").is_not_null())
ED = ED.select(["Zip", "Dim2Value", "Date ", "Count", "Cause"])
ED = ED.with_columns(pl.col('Date ').cast(pl.Utf8)) # Create a Polars Series

# function to check and convert the date format
def convert_date_format(date_str):
    # Define the regex pattern for the form "m/d/yy"
    pattern = r'^\d{1,2}/\d{1,2}/\d{2}$'
    if re.match(pattern, date_str):
        # Convert the string to a datetime object using the `time` module
        date_obj = time.strptime(date_str, '%m/%d/%y')
        # Return the date in the new format
        return time.strftime('%d-%m-%y', date_obj)
    return date_str

# Apply the function to the Date series using `map_elements`
date       = ED['Date '].map_elements(convert_date_format, return_dtype = pl.Utf8)
ED         = ED.with_columns(date.alias('Date ')) # Update the DataFrame with the converted date series
ED         = ED.rename({
                "Dim2Value": "Age_Group",
                "Date ": "Date",
                "Count": "Count",
                "Cause": "Anticipated_Cause"
                })
# Convert the 'Date' column to a proper date format and extract month and year
ED = ED.with_columns([pl.col("Date").str.strptime(pl.Date, format = "%d-%m-%y").alias("Date")])
ED = ED.with_columns([
            pl.col("Date").dt.year().alias("Year"),
            pl.col("Date").dt.month().alias("Month"),
            pl.col("Date").dt.day().alias("Day")
        ])
ED = ED.pivot(values  = "Count",
              index   = ["Zip", "Date", "Year", "Month", "Day"], #"Anticipated_Cause",
              columns = "Age_Group",
              aggregate_function = "sum")
ED = ED.fill_nan(0)
ED = ED.fill_null(0)

ED = ED.rename({"Zip": "Zip",
                "Date": "Date",
                #"Anticipated_Cause": "Anticipated_Cause",
                "Month": "Month",
                "Year": "Year",
                "Day": "Day",
                "All age groups": "Total",
                "Ages 0-4 years": "Ages0to4",
                "Ages 5-17 years": "Ages5to17",
                "Ages 18-64 years": "Ages18to64",
                "Ages 65+ years": "Ages65Plus"
                })
ED = ED.select(["Date", "Year", "Month", "Day", "Zip",# "Anticipated_Cause",
                "Ages0to4", "Ages5to17", "Ages18to64", "Ages65Plus", "Total"
              ])

# Output the combined data into a new Excel file
output_file = cwd + f"\\Data\\Daily Data\\ER visits.csv"  # Name of the output file
ED.write_csv(output_file)  # Export to csv without row indices

print(f"Combined data has been exported to {output_file}")
