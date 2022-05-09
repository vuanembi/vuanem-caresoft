from typing import Any

from compose import compose

from caresoft.pipeline.interface import Pipeline
from caresoft.repo import Data
from db.bigquery import load


def load_callback_service(pipeline: Pipeline):
    def _svc(rows: Data) -> dict[str, Any]:
        return {
            "output_rows": load(
                pipeline.table,
                pipeline.schema,
                pipeline.id_key,
                pipeline.cursor_key,
                rows,
            ),
            "callback_res": pipeline.callback_fn(pipeline.table, rows),
        }
    
    return _svc


def pipeline_service(pipeline: Pipeline, body: dict[str, Any]):
    return compose(
        load_callback_service(pipeline),
        pipeline.transform,
        pipeline.get,
        pipeline.params_fn,
    )(body)
