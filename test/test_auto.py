from .utils import process


def test_tickets_auto():
    data = {"table": "Tickets"}
    process(data)


def test_tickets_details_auto():
    data = {"table": "TicketsDetails"}
    process(data)


def test_contacts_details_auto():
    data = {"table": "ContactsDetails"}
    process(data)


def test_contacts_auto():
    data = {"table": "Contacts"}
    process(data)
