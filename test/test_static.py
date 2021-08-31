import pytest

from .utils import process


@pytest.mark.parametrize(
    "table",
    [
        "Agents",
        # "Groups",
        # "Services",
        # "TicketsCustomFields",
        # "ContactsCustomFields",
    ],
)
def test_static(table):
    data = {
        "table": table,
    }
    process(data)

