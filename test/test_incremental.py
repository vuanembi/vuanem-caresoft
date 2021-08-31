import pytest

from .utils import process

START = "2021-08-01"
END = "2021-08-02"
TABLES = (
    "table",
    [
        # "Tickets",
        # "Contacts",
        "Calls",
    ],
)


@pytest.mark.parametrize(
    *TABLES,
)
def test_incremental_auto(table):
    data = {
        "table": table,
    }
    process(data)


@pytest.mark.parametrize(
    *TABLES,
)
def test_incremental_manual(table):
    data = {
        "table": table,
        "start": START,
        "end": END,
    }
    process(data)
