import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import requests
from io import StringIO


def ExtractData(url):
    print("Performing data extraction from the url provided ")
    path_to_data = url
    try:
        response = requests.get(path_to_data)
        response.raise_for_status()
        csv_data = StringIO(response.text)
        original_df = pd.read_csv(csv_data, delimiter=';')
        # print(original_df)
    except Exception as ex:
        print("File reading failed because ", str(ex))
        return None
    print("Extraction completed successfully ")

    return original_df


def TransformData(Data_TrainCSV):
    print("Performing Data Transformation ")
    try:

        # Drop Status column
        Data_TrainCSV = Data_TrainCSV.drop('Status', axis=1)

        # Drop invalid values rows
        Data_TrainCSV['Laenge'] = Data_TrainCSV['Laenge'].str.replace(',', '.').astype(float)
        Data_TrainCSV['Breite'] = Data_TrainCSV['Breite'].str.replace(',', '.').astype(float)

        # Validate "Verkehr", "Laenge", "Breite", "IFOPT" values
        # Drop empty cells

        Data_TrainCSV = Data_TrainCSV[
            (Data_TrainCSV["Verkehr"].isin(["FV", "RV", "nur DPN"])) &
            (Data_TrainCSV["Laenge"].between(-90, 90)) &
            (Data_TrainCSV["Breite"].between(-90, 90)) &
            (Data_TrainCSV["IFOPT"].str.match(r"^[A-Za-z]{2}:\d+:\d+(?::\d+)?$"))
            ].dropna()

        # Change data type
        data_type = {
            "EVA_NR": int,
            "DS100": str,
            "IFOPT": str,
            "NAME": str,
            "Verkehr": str,
            "Laenge": float,
            "Breite": float,
            "Betreiber_Name": str,
            "Betreiber_Nr": int
        }
        # Store changed data
        Data_Train_Transformed = Data_TrainCSV.astype(data_type)
    except Exception as ex:
        print("Error occurred during data transformation: ", str(ex))
        return None

    print("Transformation completed ")
    return Data_Train_Transformed


def LoadData(Data_Train_Transformed, table_name):
    print("Creating SQLite DB from transformed data ")
    try:
        engine = create_engine("sqlite:///trainstops.sqlite")
        Data_Train_Transformed.to_sql(table_name, engine, if_exists="replace", index=False)
        print("Database created successfully ")
    except Exception as ex:
        print("Error: ", str(ex))


def driver():
    url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    Data_TrainCSV = ExtractData(url)
    if Data_TrainCSV is not None:
        Data_Train_Transformed = TransformData(Data_TrainCSV)
        if Data_Train_Transformed is not None:
            LoadData(Data_Train_Transformed, "trainstops")
        else:
            print("Data transformation failed")
    else:
        print("Data extraction failed")


if __name__ == "__main__":
    driver()