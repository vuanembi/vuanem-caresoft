from typing import Any
from datetime import datetime, timedelta

from db.bigquery import get_last_timestamp

DATE_FORMAT = "%Y-%m-%d"
API_COUNT = 500


def dimension(*args):
    def _parse(*args):
        return None

    return _parse


def _listing_request_parser(start_key: str, end_key: str):
    def _parse(table, cursor_key):
        def __parse(body: dict[str, Any]) -> dict:
            _start = body.get("start")
            _end = body.get("end")
            if _start and _end:
                start, end = [datetime.strptime(i, DATE_FORMAT) for i in [_start, _end]]
            else:
                start = get_last_timestamp(table, cursor_key)
                end = datetime.utcnow() + timedelta(hours=7)
            return {
                start_key: start.replace(tzinfo=None).isoformat(timespec="seconds")
                + "Z",
                end_key: end.isoformat(timespec="seconds") + "Z",
                "count": API_COUNT,
            }

        return __parse

    return _parse


time = _listing_request_parser("start_time_since", "start_time_to")
updated = _listing_request_parser("updated_since", "updated_to")


def details(*args):
    def _parse(body: dict[str, list[int]]):
        return body["ids"]

    return _parse
