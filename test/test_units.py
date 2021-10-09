import pytest

from unittest.mock import Mock

from main import main
from models.models import TABLES

START, END = ("2021-10-09", "2021-10-10")


def run(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    return res["results"]


class TestPipelines:
    def assert_pipelines(self, res):
        assert res["num_processed"] >= 0
        if res["num_processed"] > 0:
            for i in res["loads"]:
                assert res["num_processed"] == i["output_rows"]

    @pytest.mark.parametrize(
        "table",
        TABLES["static"],
    )
    def test_static(self, table):
        data = {
            "table": table,
        }
        self.assert_pipelines(run(data))

    @pytest.mark.parametrize(
        "table",
        TABLES["incre"],
    )
    @pytest.mark.parametrize(
        ("start", "end"),
        [
            (None, None),
            (START, END),
        ],
        ids=[
            "auto",
            "manual",
        ],
    )
    def test_incre(self, table, start, end):
        data = {
            "table": table,
            "start": start,
            "end": end,
        }
        self.assert_pipelines(run(data))

    @pytest.mark.parametrize(
        "table",
        TABLES["details"],
    )
    def test_details(self, table):
        data = {
            "table": table,
        }
        self.assert_pipelines(run(data))


@pytest.mark.parametrize(
    "tasks",
    [
        "static",
        "incre",
    ],
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (START, END),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_tasks(tasks, start, end):
    if tasks == 'static' and (start and end):
        pytest.skip()
    res = run(
        {
            "tasks": tasks,
            "start": start,
            "end": end,
        }
    )
    assert res["tasks"] > 0
