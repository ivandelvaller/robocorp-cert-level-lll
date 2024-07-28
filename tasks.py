from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables

http = HTTP()
json = JSON()
table = Tables()


@task
def produce_traffic_data():
    """Produce the data."""
    print("Producing.")
    download_traffic_data()
    data_table = load_traffic_data_as_table()


@task
def consume_traffic_data():
    """Consume the data from traffic."""
    print("Consume")


# FUNCTIONS


def download_traffic_data():
    """Download data from API and save it as a file in output folder."""
    url = "https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json"
    target_file = "output/traffic.json"

    http.download(url=url, target_file=target_file, overwrite=True)


def load_traffic_data_as_table():
    """Get data from output folder and transform it to a table."""
    data = json.load_json_from_file("output/traffic.json")
    data_table = table.create_table(data["value"])
    return data_table
