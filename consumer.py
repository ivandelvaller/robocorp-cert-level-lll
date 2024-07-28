import requests

from robocorp import workitems
from robocorp.tasks import task


@task
def consume_traffic_data():
    """Consume the data from traffic."""
    process_traffic_data()


def process_traffic_data():
    """Process the work items."""
    for item in workitems.inputs:
        traffic_data = item.payload["traffic_data"]
        if traffic_data["country"] == 3:
            status, json_response = post_traffic_data_to_sales_system(traffic_data)
            if status == 200:
                item.done()
            else:
                item.fail(
                    exception_type="APPLICATION",
                    code="TRAFFIC_DATA_POST_FAILED",
                    message=json_response["message"],
                )
        else:
            item.fail(
                exception_type="BUSINESS",
                code="INVALID_TRAFFIC_DATA",
                message=item.payload,
            )


def post_traffic_data_to_sales_system(traffic_data):
    url = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"
    response = requests.post(url, json=traffic_data)
    return response.status_code, response.json()
