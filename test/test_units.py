import pytest

from unittest.mock import Mock

from main import main
from controller.orchestrator import TABLES

START, END = ("2021-10-16", "2021-10-22")


def run(data):
    return main(Mock(get_json=Mock(return_value=data), args=data))["results"]


class TestPipelines:
    def assert_pipelines(self, res):
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
            ("2021-11-01", "2021-11-02"),
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


#     @pytest.mark.parametrize(
#         "table",
#         TABLES["incre"],
#     )
#     @pytest.mark.parametrize(
#         ("start", "end"),
#         [
#             (None, None),
#             # (START, END),
#         ],
#         ids=[
#             "auto",
#             # "manual",
#         ],
#     )
#     def test_incre(self, table, start, end):
#         data = {
#             "table": table,
#             "start": start,
#             "end": end,
#         }
#         self.assert_pipelines(run(data))

#     @pytest.mark.parametrize(
#         "table",
#         TABLES["details"],
#     )
#     def test_details(self, table):
#         data = {
#             "table": table,
#         }
#         self.assert_pipelines(run(data))


# @pytest.mark.parametrize(
#     "tasks",
#     [
#         "static",
#         "incre",
#     ],
# )
# @pytest.mark.parametrize(
#     ("start", "end"),
#     [
#         (None, None),
#         (START, END),
#     ],
#     ids=[
#         "auto",
#         "manual",
#     ],
# )
# def test_tasks(tasks, start, end):
#     if tasks == 'static' and (start and end):
#         pytest.skip()
#     res = run(
#         {
#             "tasks": tasks,
#             "start": start,
#             "end": end,
#         }
#     )
#     assert res["tasks"] > 0
