from typing import Any

from caresoft.pipeline import pipelines
from caresoft.caresoft_service import pipeline_service


def caresoft_controller(body: dict[str, Any]):
    return pipeline_service(pipelines["table"], body)
