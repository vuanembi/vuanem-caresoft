from typing import Callable, TypedDict
import asyncio
from datetime import datetime

from libs.caresoft import get_simple, get_incremental, get_many_details
from libs.bigquery import get_time_range, load_simple, load_incremental


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
    params_builder: Callable[[datetime, datetime], dict]
    keys: Keys


class CaresoftDetails(Caresoft):
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


def incremental_pipelines(model: CaresoftIncremental) -> Pipelines:
    def run(dataset, request_data) -> dict:
        data = get_incremental(
            model["endpoint"],
            model["row_key"],
            model["params_builder"],
            *get_time_range(
                dataset,
                model["name"],
                model["keys"]["p_key"],
                request_data.get("start"),
                request_data.get("end"),
            ),
        )
        output_rows = load_incremental(
            dataset,
            model["name"],
            model["schema"],
            model["keys"],
            model["transform"](data),
        )
        ids = [i[model["keys"]["p_key"]] for i in data]
        tasks = [
            {
                "table": f"{model['name']}Details",
                "ids": ids[i : i + DETAIS_LIMIT],
            }
            for i in range(0, len(ids), DETAIS_LIMIT)
        ]
        # create_tasks()
        return {
            "table": model["name"],
            "output_rows": output_rows,
        }

    return run


def details_pipelines(model: CaresoftDetails) -> Pipelines:
    def run(dataset, request_data) -> dict:
        return {
            "table": model["name"],
            "output_rows": load_incremental(
                dataset,
                model["name"],
                model["schema"],
                model["keys"],
                model["transform"](
                    asyncio.run(
                        get_many_details(
                            model["endpoint"],
                            model["row_key"],
                            request_data["ids"],
                        )
                    )
                ),
            ),
        }

    return run
