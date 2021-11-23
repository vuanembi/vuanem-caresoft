import os
import json

import requests

from controller.pipelines import factory, run
# from tasks import create_tasks

DATASET = "Caresoft"


def main(request):
    """API Gateway

    Args:
        request (flask.request): Incoming Request

    Returns:
        dict: Responses
    """

    data = request.get_json()
    print(data)

    if "tasks" in data:
        # results = create_tasks(data)
        pass
    elif "table" in data:
        results = run(DATASET, factory(data["table"]), data)

    response = {
        "pipelines": "Caresoft",
        "results": results,
    }

    print(response)

    requests.post(
        "https://api.telegram.org/bot{token}/sendMessage".format(
            token=os.getenv("TELEGRAM_TOKEN")
        ),
        json={
            "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
            "text": json.dumps(response, indent=4),
        },
    )
    return response
