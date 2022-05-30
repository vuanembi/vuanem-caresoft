from typing import Any

from caresoft.pipeline import pipelines, details_pipelines
from caresoft.caresoft_service import pipeline_service


def caresoft_controller(body: dict[str, Any]):
    return pipeline_service((pipelines | details_pipelines)[body["table"]], body)
