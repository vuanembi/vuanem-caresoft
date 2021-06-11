from unittest.mock import Mock

from main import main

from .utils import assertion, encode_data


def test_tickets_manual():
    data = {
        "table": "Tickets",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)

def test_tickets_auto():
    data = {
        "table": "Tickets"
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_contacts_manual():
    data = {
        "table": "Contacts",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)

def test_contacts_auto():
    data = {
        "table": "Contacts"
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_calls_manual():
    data = {
        "table": "Calls",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)

def test_calls_auto():
    data = {
        "table": "Calls"
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_agents():
    data = {"table": "Agents"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_groups():
    data = {"table": "Groups"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_services():
    data = {"table": "Services"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_tickets_custom_fields():
    data = {"table": "TicketsCustomFields"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_contacts_custom_fields():
    data = {"table": "ContactsCustomFields"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)
    

def test_broadcast():
    data = {"broadcast": True}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assert res["message_sent"] > 0
