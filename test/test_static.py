from .utils import process


def test_calls_auto():
    data = {"table": "Calls"}
    process(data)


def test_agents():
    data = {"table": "Agents"}
    process(data)


def test_groups():
    data = {"table": "Groups"}
    process(data)


def test_services():
    data = {"table": "Services"}
    process(data)


def test_tickets_custom_fields():
    data = {"table": "TicketsCustomFields"}
    process(data)


def test_contacts_custom_fields():
    data = {"table": "ContactsCustomFields"}
    process(data)
