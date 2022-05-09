from typing import Callable, Any

from caresoft.pipeline import pipelines
from caresoft.repo import DETAILS_LIMIT, Data
from tasks.cloud_tasks import create_tasks


def create_cron_tasks_service(body: dict[str, str]) -> dict[str, int]:
    return {
        "tasks": create_tasks(
            "caresoft",
            [
                {
                    "table": table,
                    "start": body.get("start"),
                    "end": body.get("end"),
                }
                for table in pipelines.keys()
            ],
            lambda x: x["table"],
        )
    }


def create_details_tasks_service(id_fn: Callable[[dict[str, Any]], int]):
    def _svc(table: str, rows: Data):
        ids = [id_fn(id) for id in rows]
        return {
            "tasks": create_tasks(
                "caresoft-details",
                [
                    {
                        "table": table,
                        "ids": ids[i : i + DETAILS_LIMIT],
                    }
                    for i in range(0, len(ids), DETAILS_LIMIT)
                ],
                lambda x: x["table"],
            )
        }

    return _svc
