import pytest

from .utils import process


@pytest.mark.parametrize(
    "table",
    [
        "ContactsDetails",
        "TicketsDetails",
    ],
)
def test_details(table):
    data = {
        "table": table,
    }
    process(data)
