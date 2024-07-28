# SETTINGS

from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables
import json

# KEYWORDS

http = HTTP()
json_rpa = JSON()
table = Tables()

TRAFFIC_JSON_FILE_PATH = "output/traffic.json"

# TASKS


@task
def produce_traffic_data():
    """Produce the data."""
    print("Producing.")
    download_traffic_data()
    data_table = load_traffic_data_as_table()
    filtered_data = filter_and_sort_traffic_data(data_table)
    get_latest_data_by_country(filtered_data)


@task
def consume_traffic_data():
    """Consume the data from traffic."""
    print("Consume")


# FUNCTIONS


def download_traffic_data():
    """Download data from API and save it as a file in output folder."""
    url = "https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json"

    http.download(url=url, target_file=TRAFFIC_JSON_FILE_PATH, overwrite=True)


def load_traffic_data_as_table():
    """Get data from output folder and transform it to a table."""
    data = json_rpa.load_json_from_file("output/traffic.json")
    data_table = table.create_table(data["value"])
    return data_table


def filter_and_sort_traffic_data(data_table):
    """Get data from file to filter and sort it."""
    # Iterate data and filter the three columns.
    filtered_data = []

    # KEYS
    rate_key = "NumericValue"
    gender_key = "Dim1"
    year_key = "TimeDim"
    code_country_key = "SpatialDim"

    # VALUES
    max_rate = 5.0
    gender_value = "BTSX"

    # FILTER DATA
    for data in data_table:
        if data[rate_key] < max_rate and data[gender_key] == gender_value:
            item = {}
            item[rate_key] = data[rate_key]
            item[gender_key] = data[gender_key]
            item[year_key] = data[year_key]
            item[code_country_key] = data[code_country_key]
            filtered_data.append(item)

    # SORT DATA BY TimeDim
    sorted_data = sorted(filtered_data, key=lambda x: x[year_key], reverse=True)

    return sorted_data


def get_latest_data_by_country(data):
    """Get the latest data of each country in data array."""
    # Group data by country code.
    country_code_key = "SpatialDim"
    latest_data = []
    countries = set()

    for row in data:
        code = row[country_code_key]
        country_code = {code}
        exists = countries.intersection(country_code)

        if exists:
            pass
        else:
            countries.add(code)
            latest_data.append(row)
            pass

    return latest_data
