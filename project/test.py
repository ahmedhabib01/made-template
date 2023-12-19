import pandas as pd
from sqlalchemy import create_engine, inspect
import pipeline

def test_data_extract(path):
    df = pipeline.Extract_Data(path)
    assert not df.empty, "Data Extraction Failed"
    print("test_data_extract: Test Passed")
    return df


def test_load_data(table_name, db_file):
    engine = create_engine(f"sqlite:///./data/{db_file}")
    inspector = inspect(engine)

    exists = inspector.has_table(table_name)
    assert exists, f"The table '{table_name}' does not exist in the database."

    print("test_load_data: Table exists, Test Passed")

def Test_Pipeline():
    parking_violation_url = "https://opendata.bonn.de/sites/default/files/Parkverstoesse2022.csv"
    test_data_extract(parking_violation_url)

    street_directory_url = "https://stadtplan.bonn.de/csv?Thema=17790"
    test_data_extract(street_directory_url)

    db_file = 'Traffic.sqlite'

    test_load_data('Parking_Violations', db_file)
    test_load_data('Street_Directory', db_file)

if __name__ == "__main__":
    Test_Pipeline()
