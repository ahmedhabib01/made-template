import zipfile
import pandas as pd
from urllib.request import urlretrieve
import os
import sqlite3

# Download and unzip data
zip_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
download_path = "mowesta-dataset.zip"
extracted_folder = "mowesta-dataset"

# Download ZIP file
urlretrieve(zip_url, download_path)

# Extract ZIP file
with zipfile.ZipFile(download_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_folder)

# Reshape data
csv_file_path = os.path.join(extracted_folder, "data.csv")
df = pd.read_csv(csv_file_path)

# Keep selected columns
selected_columns = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]
df = df[selected_columns]

# Rename columns
df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"}, inplace=True)

# Transform data
# Convert temperatures to Fahrenheit
df["Temperatur"] = (df["Temperatur"] * 9/5) + 32
df["Batterietemperatur"] = (df["Batterietemperatur"] * 9/5) + 32

# Validate data
# Example validation: Check if "Geraet" is a positive integer
df = df[df["Geraet"].astype(str).str.isdigit() & (df["Geraet"] > 0)]

# Write data into SQLite database
db_path = "temperatures.sqlite"
table_name = "temperatures"

# Connect to SQLite database
conn = sqlite3.connect(db_path)

# Write DataFrame to SQLite table
df.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the database connection
conn.close()

# Clean up: Remove downloaded ZIP file and extracted folder
os.remo
