import pytest

from caresoft.pipeline import dimension_pipelines, listing_pipelines, details_pipelines
from caresoft.caresoft_service import pipeline_service
from tasks.tasks_service import create_cron_tasks_service

test_details_data = {
    "TicketsDetails": [
        264603737,
        264603709,
        264603635,
        264603585,
        264603520,
        264603377,
        264603370,
        264603344,
        264603328,
        245029656,
    ],
    "ContactsDetails": [
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
}

TIMEFRAME = (
    # ("auto", (None, None)),
    ("manual", ("2022-05-01", "2022-05-11")),
)


@pytest.fixture(
    params=[t[1] for t in TIMEFRAME],
    ids=[t[0] for t in TIMEFRAME],
)
def timeframe(request):
    return request.param


class TestCaresoft:
    def assert_pipelines(self, pipeline, body):
        res = pipeline_service(pipeline, body)
        assert res["output_rows"] >= 0

    @pytest.mark.parametrize(
        "pipeline",
        argvalues=dimension_pipelines.values(),
        ids=dimension_pipelines.keys(),
    )
    def test_dimensions(self, pipeline):
        self.assert_pipelines(pipeline, {})

    @pytest.mark.parametrize(
        "pipeline",
        argvalues=listing_pipelines.values(),
        ids=listing_pipelines.keys(),
    )
    def test_incremental(self, pipeline, timeframe):
        self.assert_pipelines(
            pipeline,
            {
                "start": timeframe[0],
                "end": timeframe[1],
            },
        )

    @pytest.mark.parametrize(
        "pipeline",
        argvalues=details_pipelines.values(),
        ids=details_pipelines.keys(),
    )
    def test_details(self, pipeline):
        self.assert_pipelines(
            pipeline,
            {"ids": test_details_data[pipeline.name]},
        )


class TestTasks:
    def test_service(self, timeframe):
        res = create_cron_tasks_service(
            {
                "start": timeframe[0],
                "end": timeframe[1],
            }
        )
        assert res["tasks"] > 0
