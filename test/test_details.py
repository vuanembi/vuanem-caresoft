import pytest

from .utils import process


@pytest.mark.parametrize(
    "table",
    [
        "TicketsDetails",
        "ContactsDetails",
    ],
)
def test_details(table):
    data = {
        "table": table,
    }
    process(data)
