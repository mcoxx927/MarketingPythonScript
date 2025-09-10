
import pytest
import pandas as pd
import os
from tools.territory_extraction import mark_parcels_in_territory

@pytest.fixture
def setup_test_files():
    # Create a temporary directory for test output
    test_output_dir = r"C:\Users\1\Documents\GitHub\MarketingPythonScript\tests\temp_output"
    os.makedirs(test_output_dir, exist_ok=True)
    yield test_output_dir
    # Teardown: Clean up the directory
    for file in os.listdir(test_output_dir):
        os.remove(os.path.join(test_output_dir, file))
    os.rmdir(test_output_dir)

def test_mark_parcels_in_territory(setup_test_files):
    test_output_dir = setup_test_files
    kml_file = r"C:\Users\1\Documents\GitHub\MarketingPythonScript\tests\test_data\test_territory.kml"
    parcels_file = r"C:\Users\1\Documents\GitHub\MarketingPythonScript\tests\test_data\test_parcels.geojson"
    output_file = os.path.join(test_output_dir, "test_output.csv")

    mark_parcels_in_territory(kml_file, parcels_file, output_file)

    # Verify the output
    result_df = pd.read_csv(output_file, dtype={'Parcel_ID': str})

    # Sort by Parcel_ID to ensure consistent order
    result_df = result_df.sort_values(by='Parcel_ID').reset_index()

    assert len(result_df) == 3
    assert result_df.loc[0]['Parcel_ID'] == '1'
    assert result_df.loc[0]['within_territory'] == True
    assert result_df.loc[1]['Parcel_ID'] == '2'
    assert result_df.loc[1]['within_territory'] == False
    assert result_df.loc[2]['Parcel_ID'] == '3'
    assert result_df.loc[2]['within_territory'] == False
