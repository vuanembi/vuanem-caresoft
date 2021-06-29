import json
import base64
from unittest.mock import Mock

from main import main

def encode_data(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    return {"message": {"data": data_encoded}}

def assertion(res):
    for i in res:
        assert i["num_processed"] > 0
        assert i["output_rows"] > 0
        assert i["num_processed"] == i["output_rows"]
        assert i["errors"] is None

def process(data):
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)
