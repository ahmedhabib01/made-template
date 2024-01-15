import zipfile
import pandas as pd
from urllib.request import urlretrieve
import os
import sqlite3
import sqlalchemy
from sqlalchemy import create_engine

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


print("Creating SQLite DB from data ")
table_name = "temperatures"
engine = create_engine(f"sqlite:///temperatures.sqlite")
df.to_sql(table_name, engine, if_exists="replace", index=False)
print("Database created successfully ")
