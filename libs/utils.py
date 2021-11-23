from typing import Callable
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
API_COUNT = 500
API_REQ_PER_SEC = 15

def params_builder(start_key: str, end_key: str) -> Callable[[datetime, datetime], dict]:
    def build(start: datetime, end: datetime) -> dict:
        return {
            start_key: start.strftime(TIMESTAMP_FORMAT),
            end_key: end.strftime(TIMESTAMP_FORMAT),
            "count": API_COUNT,
        }

    return build
