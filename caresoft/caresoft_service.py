from typing import Any

from compose import compose

from caresoft.pipeline.interface import Pipeline
from caresoft.repo import Data
from db.bigquery import load


def load_callback_service(pipeline, callback_fn):
    def _svc(rows: Data) -> dict[str, Any]:
        return {
            "output_rows": load(
                pipeline.table,
                pipeline.schema,
                pipeline.id_key,
                pipeline.cursor_key,
                rows,
            ),
            "callback_res": callback_fn(rows),
        }
    
    return _svc


def pipeline_service(pipeline: Pipeline, body: dict[str, Any]):
    return compose(
        pipeline.params_fn,
        pipeline.get,
    )(body)
