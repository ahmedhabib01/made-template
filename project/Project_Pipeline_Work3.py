import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import requests
from io import StringIO


def Extract_Data(file_path):
    url = file_path
    response = requests.get(url)
    response.raise_for_status()
    csvdata = StringIO(response.text)
    original_df = pd.read_csv(csvdata, delimiter=';')
    return original_df


def Transform_ParkingViolation(data, selected_columns=None):
    if selected_columns:
        data_selected = data[selected_columns]
        data_selected.rename(columns={
            'TATZEIT': 'Crime_Str_No',
            'TATORT': 'Crime_location',
            'TATBESTANDBE_TBNR': 'Crime_Violation_No',
            'GELDBUSSE': 'Total_Fine',
            'BEZEICHNUNG': 'Vehicle_Description'
        }, inplace=True)
        return data_selected
    else:
        return data


def Transform_StreetDirectory(data, selected_columns=None):
    if selected_columns:
        data_selected = data[selected_columns]
        data_selected.rename(columns={'strassen_bez': 'Street_Name', 'strasse': 'Street_Number'}, inplace=True)
        return data_selected
    else:
        return data


def Load_DB(data, table_name):
    
    engine = create_engine("sqlite:///./data/Traffic.sqlite")
    data.to_sql(table_name, engine, if_exists="replace")


def ETL_Drive():
    parking_violation_url = "https://opendata.bonn.de/sites/default/files/Parkverstoesse2022.csv"
    Data_ParkingViolation = Extract_Data(parking_violation_url)

    # Transform data from the first csv file by selecting the desired columns
    selected_columns = ['TATZEIT', 'TATORT', 'TATBESTANDBE_TBNR', 'GELDBUSSE', 'BEZEICHNUNG']
    ParkingViolation_Selected = Transform_ParkingViolation(Data_ParkingViolation, selected_columns)
    # print(ParkingViolation_Selected)

    street_directory_url = "https://stadtplan.bonn.de/csv?Thema=17790"
    Data_StreetDirectory = Extract_Data(street_directory_url)

    # Transform data from the second csv file by selecting the desired columns
    selected_columns = ['strasse', 'strassen_bez']
    StreetDirectory_Selected = Transform_StreetDirectory(Data_StreetDirectory, selected_columns)
    # print(StreetDirectory_Selected)

    Database = 'Traffic.sqlite'
    Load_DB(ParkingViolation_Selected, 'Parking_Violations')
    Load_DB(StreetDirectory_Selected, 'Street_Directory')


if __name__ == "__main__":
    ETL_Drive()
