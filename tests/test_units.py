from unittest.mock import Mock
from datetime import datetime

from main import main

from .utils import assertion, encode_data


def test_tickets():
    data = {
        "mode": "manual",
        "table": "Tickets",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_contacts():
    data = {
        "mode": "manual",
        "table": "Contacts",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)

def test_calls():
    data = {
        "mode": "manual",
        "table": "Calls",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)

def test_manual2():
    data = {"table": "Tickets", "start": "2021-06-01", "end": "2021-06-02"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_manual2():
    data = {"table": "Tickets", "start": "2021-06-01", "end": "2021-06-02"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)
