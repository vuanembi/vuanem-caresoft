from typing import Any

from compose import compose

from caresoft.pipeline.interface import Pipeline
from caresoft.repo import Data
from tasks.tasks_service import create_details_tasks_service
from db.bigquery import load


def load_callback_service(pipeline: Pipeline):
    def _svc(rows: Data) -> dict[str, Any]:
        return {
            "output_rows": load(
                pipeline.name,
                pipeline.schema,
                pipeline.id_key,
                pipeline.partition_key,
                rows,
            ),
            **(
                {
                    "callback_res": create_details_tasks_service(
                        pipeline.name,
                        pipeline.id_key,
                        rows,
                    )
                }
                if pipeline.queue_task and pipeline.id_key
                else {}
            ),
        }

    return _svc


def pipeline_service(pipeline: Pipeline, body: dict[str, Any]):
    return compose(
        load_callback_service(pipeline),
        pipeline.transform,
        pipeline.get,
        pipeline.params_fn(pipeline.name, pipeline.cursor_key),
    )(body)
