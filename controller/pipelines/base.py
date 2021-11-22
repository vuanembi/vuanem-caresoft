import importlib
import asyncio

from models.Caresoft.base import CaresoftResource
from libs.caresoft import get_dimensions, get_incremental, get_many_details
from libs.bigquery import get_time_range


def factory(table):
    try:
        return getattr(importlib.import_module(f"models.Caresoft.{table}"), table)
    except (ImportError, AttributeError):
        raise ValueError(table)


def transform_and_load(model: CaresoftResource, data: list[dict]) -> dict:
    response = {
        "table": model["name"],
        "num_processed": len(data),
    }
    if len(data) > 0:
        response["loads"] = [loader(model, data) for loader in model["loader"]]
    return response


def dimension_controller(model):
    return transform_and_load(model, get_dimensions(model))


def incremental_controller(model, start, end):
    data = get_incremental(model, *get_time_range(start, end))
    response = transform_and_load(model, data)
    # create_tasks([data[model['detail_key']])


def details_controller(model, ids):
    return transform_and_load(model, asyncio.run(get_many_details(model, ids)))
