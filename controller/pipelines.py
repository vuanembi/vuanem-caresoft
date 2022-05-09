import importlib

from models.base import Pipelines


def factory(table: str) -> Pipelines:
    try:
        return getattr(importlib.import_module(f"models.{table}"), table)
    except (ImportError, AttributeError):
        raise ValueError(table)


def run(dataset: str, model: Pipelines, request_data: dict) -> dict:
    return model(dataset, request_data)
