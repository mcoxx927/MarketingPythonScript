# Territory Extraction Guide

This guide explains how to use the `territory_extraction.py` script to identify which parcels from a GeoJSON file fall within the territories defined in a KML file.

## Overview

The `territory_extraction.py` script performs a spatial join between a GeoJSON file containing parcel data and a KML file containing one or more territory polygons. It then generates a CSV file with all the original parcel data and an additional `within_territory` column, which is `True` if a parcel is within a territory and `False` otherwise.

## Prerequisites

Before running the script, you need to have the following installed:

1.  **Python 3**: If you don't have Python installed, you can download it from [python.org](https://python.org).
2.  **Required Python packages**: You can install the necessary packages by running the following command in your terminal from the root of the project directory:

    ```bash
    pip install -e .
    ```

## How to Run the Script

You can run the script from the command line using the following format:

```bash
python src\tools\territory_extraction.py --kml-file "C:\Path\To\Your\territory.kml" --parcels-file "C:\Path\To\Your\parcels.geojson" --output-file "C:\Path\To\Your\output.csv"
```

### Command-Line Arguments

*   `--kml-file`: (Required) The path to the KML file that defines your territories.
*   `--parcels-file`: (Required) The path to the GeoJSON file that contains your parcel data.
*   `--output-file`: (Required) The path where you want to save the resulting CSV file.

### Example

Here is an example of how to run the script with some sample files:

```bash
python src\tools\territory_extraction.py --kml-file "C:\Users\1\Documents\GitHub\MarketingPythonScript\GeoCoding\Untitled layer.kml" --parcels-file "C:\Users\1\Documents\GitHub\MarketingPythonScript\GeoCoding\LburgParcels geojson.geojson" --output-file "C:\Users\1\Documents\GitHub\MarketingPythonScript\output\my_territory_results.csv"
```

## Output

The script will generate a CSV file at the path you specify for the `--output-file` argument. This file will contain all the columns from your original parcel data, plus a new column named `within_territory`.

*   **within_territory**: This column will have a value of `True` if the parcel is within any of the territories defined in your KML file, and `False` otherwise.
