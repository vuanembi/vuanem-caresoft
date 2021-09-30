import os
import json

import requests

from models.models import Caresoft
# from broadcast import broadcast


def main(request):
    """API Gateway

    Args:
        request (flask.request): Incoming Request

    Returns:
        dict: Responses
    """

    data = request.get_json()
    print(data)

    if "broadcast" in data:
        # results = broadcast()
        pass
    elif "table" in data:
        job = Caresoft.factory(
            data["table"],
            data.get("start"),
            data.get("end"),
        )
        results = job.run()

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
