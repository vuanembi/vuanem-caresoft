import os
import json
import base64

from google.cloud import pubsub_v1

from models import Caresoft


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))

    if "broadcast" in data:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID")
        )
        tables = [f"configs/{i}" for i in os.listdir("configs")]
        for table in tables:
            message_json = json.dumps({"table": table})
            message_bytes = message_json.encode("utf-8")
            publisher.publish(topic_path, data=message_bytes).result()
        responses = {"message_sent": len(tables)}
    else:
        job = Caresoft.factory(data["table"], data.get("start"), data.get("end"))
        responses = {"pipelines": "Caresoft", "results": job.run()}

    print(responses)
    return responses
