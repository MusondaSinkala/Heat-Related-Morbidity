import os
import polars as pl  # Importing polars instead of pandas

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
full_data = pl.concat(dfs)  # Concatenate without re-indexing manually

# Output the combined data into a new Excel file
output_file = cwd + f"\\Data\\full_data.csv"  # Name of the output file
full_data.write_csv(output_file)  # Export to csv without row indices

print(f"Combined data has been exported to {output_file}")
