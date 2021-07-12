from .utils import process


def test_tickets_manual():
    data = {
        "table": "Tickets",
        "start": "2021-07-07",
        "end": "2021-07-10",
    }
    process(data)


def test_contacts_manual():
    data = {
        "table": "Contacts",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    process(data)


def test_calls_manual():
    data = {
        "table": "Calls",
        "start": "2021-06-01",
        "end": "2021-06-02",
    }
    process(data)
