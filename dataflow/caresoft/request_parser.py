from typing import Any
from datetime import datetime

DATE_FORMAT = "%Y-%m-%d"


def dimension(*args):
    return args


def _listing_request_parser(start_key: str, end_key: str):
    def _parse(body: dict[str, Any]):
        start = body.get("start", "")
        end = body.get("end", "")

        _start, _end = [
            datetime.strptime(i, DATE_FORMAT).isoformat(timespec="seconds") + "Z"
            for i in [start, end]
        ]
        return {
            start_key: _start,
            end_key: _end,
        }

    return _parse


time = _listing_request_parser("start_time_since", "start_time_to")
updated = _listing_request_parser("updated_since", "updated_to")


def details(body: dict[str, Any]):
    return body["ids"]
