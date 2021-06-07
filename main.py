import json
import base64

from models import Caresoft


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))

    if data['mode']=='manual':
        job = Caresoft.factory(data['table'], data.get('start'), data.get('end'))

    responses = {"pipelines": "Caresoft", "results": job.run()}

    return responses


if __name__ == "__main__":
    main()
