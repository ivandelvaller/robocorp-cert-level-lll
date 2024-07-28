# SETTINGS
from robocorp import workitems
from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables

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
    latest_data = get_latest_data_by_country(filtered_data)
    payloads = create_work_item_payloads(latest_data)


@task
def consume_traffic_data():
    """Consume the data from traffic."""
    print("Consumming")
    process_traffic_data()


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
            item["rate"] = data[rate_key]
            item["year"] = data[year_key]
            item["country"] = data[code_country_key]
            item[gender_key] = data[gender_key]

            filtered_data.append(item)

    # SORT DATA BY TimeDim
    sorted_data = sorted(filtered_data, key=lambda x: x["year"], reverse=True)

    return sorted_data


def get_latest_data_by_country(data):
    """Get the latest data of each country in data array."""
    # Group data by country code.
    country_code_key = "country"
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


def create_work_item_payloads(traffic_data):
    """Clear data in array."""
    payloads = []
    for data in traffic_data:
        item = {}
        item["country"] = data["country"]
        item["year"] = data["year"]
        item["rate"] = data["rate"]

        payloads.append(item)

    return payloads


def save_work_item_payloads(payloads):
    """Save the payloads as work items."""
    for item in payloads:
        variables = {"traffic_data": item}
        workitems.outputs.create(variables)


def process_traffic_data():
    """Process the work items."""
    for item in workitems.inputs:
        traffic_data = item.payload["traffic_data"]
        print(traffic_data)
