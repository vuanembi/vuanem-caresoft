from typing import Any
from unittest.mock import Mock

import pytest

from main import main
from controller.tasks import TABLES

test_details_data: list[dict[str, Any]] = [
    {
        "table": "TicketsDetails",
        "ids": [
            264603737,
            264603709,
            264603635,
            264603585,
            264603520,
            264603377,
            264603370,
            264603344,
            264603328,
        ],
    },
    {
        "table": "ContactsDetails",
        "ids": [
            125095287,
            125095286,
            125095133,
            125095021,
            125094927,
            125094920,
            125094905,
            125094859,
            125094788,
            125094699,
            125094604,
            125094555,
            125094548,
            125094468,
        ],
    },
]


def run(data: dict) -> dict:
    return main(Mock(get_json=Mock(return_value=data), args=data))["results"]


class TestPipelines:
    def assert_pipelines(self, res: dict):
        assert res["num_processed"] >= 0
        if res["num_processed"] > 0:
            assert res["num_processed"] == res["output_rows"]

    @pytest.mark.parametrize(
        "table",
        TABLES["dimensions"],
    )
    def test_dimensions(self, table):
        self.assert_pipelines(
            run(
                {
                    "table": table,
                }
            )
        )

    @pytest.mark.parametrize(
        "table",
        TABLES["incremental"],
    )
    @pytest.mark.parametrize(
        ("start", "end"),
        [
            (None, None),
            ("2021-10-01", "2021-10-05"),
        ],
        ids=[
            "auto",
            "manual",
        ],
    )
    def test_incremental(self, table, start, end):
        self.assert_pipelines(
            run(
                {
                    "table": table,
                    "start": start,
                    "end": end,
                }
            )
        )

    @pytest.mark.parametrize(
        "data",
        test_details_data,
        ids=[i["table"] for i in test_details_data],
    )
    def test_details(self, data):
        self.assert_pipelines(run(data))


@pytest.mark.parametrize(
    "tasks",
    [
        "dimensions",
        "incremental",
    ],
)
def test_tasks(tasks):
    res = run(
        {
            "tasks": tasks,
        }
    )
    assert res["tasks_created"] > 0
