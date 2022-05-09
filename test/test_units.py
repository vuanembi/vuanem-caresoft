import pytest

from caresoft import pipeline
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
    ("auto", (None, None)),
    ("manual", ("2021-11-15", "2021-12-01")),
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
        assert res["num_processed"] >= 0
        if res["num_processed"] > 0:
            assert res["num_processed"] == res["output_rows"]

    @pytest.mark.parametrize(
        "pipeline",
        argvalues=pipeline.dimension_pipelines.values(),
        ids=pipeline.dimension_pipelines.keys(),
    )
    def test_dimensions(self, pipeline):
        self.assert_pipelines(pipeline, {})

    @pytest.mark.parametrize(
        "pipeline",
        argvalues=pipeline.listing_pipelines.values(),
        ids=pipeline.listing_pipelines.keys(),
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
        argvalues=pipeline.details_pipelines.values(),
        ids=pipeline.details_pipelines.keys(),
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
