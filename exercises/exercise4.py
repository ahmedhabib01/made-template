import urllib.request
import zipfile
import pandas as pd
import sqlite3


def Extract_data(url):
    zip_filename = 'mowesta-dataset.zip'
    data_filename = 'data.csv'
    urllib.request.urlretrieve(url, zip_filename)
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall()

    return data_filename


def Transform_data(filename):
    df = pd.read_csv(filename, sep=";", decimal=",", index_col=False,
                     usecols=["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)",
                              "Batterietemperatur in °C", "Geraet aktiv"])

    # Rename columns
    df = df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"})

    # Discard columns to the right of "Geraet aktiv"
    columns_to_keep = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur", "Batterietemperatur", "Geraet aktiv"]
    df = df[columns_to_keep]

    return df


def Transform_temp(df):
    # From Celsius to Fahrenheit
    df['Temperatur'] = (df['Temperatur'] * 9 / 5) + 32

    # From Celsius to Fahrenheit
    df['Batterietemperatur'] = (df['Batterietemperatur'] * 9 / 5) + 32

    return df


def validate_data(df):
    df = df[df['Geraet'] > 0]
    df = df[df['Hersteller'].astype(str).str.strip().ne("")]
    df = df[df['Model'].astype(str).str.strip().ne("")]
    df = df[df['Monat'].between(1, 12)]
    df = df[pd.to_numeric(df['Temperatur'], errors='coerce').notnull()]
    df = df[pd.to_numeric(df['Batterietemperatur'], errors='coerce').notnull()]
    df = df[df['Geraet aktiv'].isin(['Ja', 'Nein'])]

    return df


def Create_DB(df, database_name, table_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Geraet BIGINT,
            Hersteller TEXT,
            Model TEXT,
            Monat TEXT,
            Temperatur FLOAT,
            Batterietemperatur FLOAT,
            Geraet_aktiv TEXT
        )
    """
    cursor.execute(create_table_query)
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()


url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'

data = Extract_data(url)
transformed_df = Transform_data(data)
transformed_temp = Transform_temp(transformed_df)
validated_data = validate_data(transformed_temp)

database_name = 'temperatures.sqlite'
table_name = 'temperatures'
Create_DB(validated_data, database_name, table_name)
