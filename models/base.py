from typing import Callable, TypedDict
import asyncio
from datetime import datetime

from libs.caresoft import (
    get_simple,
    get_incremental,
    get_many_details,
    updated_params_builder,
    time_params_builder,
)
from libs.bigquery import get_time_range, load_simple, load_incremental
from libs.tasks import create_details_tasks


class Caresoft(TypedDict):
    name: str
    endpoint: str
    row_key: str
    transform: Callable[[list[dict]], list[dict]]
    schema: list[dict]


class Keys(TypedDict):
    p_key: str
    incre_key: str


class CaresoftIncremental(Caresoft):
    keys: Keys


Pipelines = Callable[[str, dict], dict]
DETAIS_LIMIT = 2500


def simple_pipelines(model: Caresoft) -> Pipelines:
    def run(dataset, request_data) -> dict:
        data = get_simple(model["endpoint"], model["row_key"])
        return {
            "table": model["name"],
            "num_processed": len(data),
            "output_rows": load_simple(
                dataset,
                model["name"],
                model["schema"],
                model["transform"](data),
            ),
        }

    return run


def get_incremental_model(
    params_builder: Callable[[datetime, datetime], dict],
    dataset: str,
    model: CaresoftIncremental,
    request_data: dict,
) -> list[dict]:
    return get_incremental(
        model["endpoint"],
        model["row_key"],
        params_builder,
        *get_time_range(
            dataset,
            model["name"],
            model["keys"]["incre_key"],
            request_data.get("start"),
            request_data.get("end"),
        ),
    )


def enqueue_details_tasks(model: CaresoftIncremental, data: list[dict]):
    ids = [i[model["keys"]["p_key"]] for i in data]
    return create_details_tasks(
        [
            {
                "table": f"{model['name']}Details",
                "ids": ids[i : i + DETAIS_LIMIT],
            }
            for i in range(0, len(ids), DETAIS_LIMIT)
        ]
    )


def incremental_time_pipelines(model: CaresoftIncremental) -> Pipelines:
    def run(dataset, request_data) -> dict:
        data = get_incremental_model(time_params_builder, dataset, model, request_data)
        return {
            "table": model["name"],
            "num_processed": len(data),
            "output_rows": load_incremental(
                dataset,
                model["name"],
                model["schema"],
                model["keys"],
                model["transform"](data),
            ),
        }

    return run


def incremental_updated_pipelines(model: CaresoftIncremental) -> Pipelines:
    def run(dataset, request_data) -> dict:
        data = get_incremental_model(
            updated_params_builder,
            dataset,
            model,
            request_data,
        )
        return {
            "table": model["name"],
            "num_processed": len(data),
            "output_rows": load_incremental(
                dataset,
                model["name"],
                model["schema"],
                model["keys"],
                model["transform"](data),
            ),
            "task_created": enqueue_details_tasks(model, data),
        }

    return run


def details_pipelines(model: CaresoftIncremental) -> Pipelines:
    def run(dataset, request_data) -> dict:
        data = asyncio.run(
            get_many_details(
                model["endpoint"],
                model["row_key"],
                request_data["ids"],
            )
        )
        return {
            "table": model["name"],
            "num_processed": len(data),
            "output_rows": load_incremental(
                dataset,
                model["name"],
                model["schema"],
                model["keys"],
                model["transform"](data),
            ),
        }

    return run
