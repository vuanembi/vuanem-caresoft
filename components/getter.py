import sys
import asyncio
import math
from abc import ABCMeta, abstractmethod
from datetime import datetime

import requests
import aiohttp
from google.api_core.exceptions import NotFound

from config import (
    BASE_URL,
    HEADERS,
    COUNT,
    BQ_CLIENT,
    DATASET,
    DETAILS_LIMIT,
    NOW,
    DATE_FORMAT,
    TIMESTAMP_FORMAT,
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class Getter(metaclass=ABCMeta):
    def __init__(self, model):
        self.endpoint = model.endpoint
        self.row_key = model.row_key

    @abstractmethod
    def get(self):
        pass


class SimpleGetter(Getter):
    def get(self):
        with requests.get(
            f"{BASE_URL}/{self.endpoint}",
            headers=HEADERS,
        ) as r:
            res = r.json()
        return res[self.row_key]


class IncrementalGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.table = model.table
        self.keys = model.keys
        self.start, self.end = self._get_time_range(model.start, model.end)

    @property
    @abstractmethod
    def params(self):
        pass

    def get(self):
        return asyncio.run(self._get())

    def _get_time_range(self, start, end):
        if start and end:
            start, end = [datetime.strptime(i, DATE_FORMAT) for i in [start, end]]
        else:
            end = NOW
            try:
                query = f"""
                SELECT MAX({self.keys['incre_key']}) AS incre
                FROM `{DATASET}.{self.table}`
                """
                rows = BQ_CLIENT.query(query).result()
                row = [row for row in rows][0]
                max_incre = row["incre"]
            except (KeyError, NotFound):
                max_incre = datetime(2020, 6, 1)
            start = max_incre
        return start, end

    async def _get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        connector = aiohttp.TCPConnector(limit=3)
        timeout = aiohttp.ClientTimeout(total=540)
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as session:
            num_found = await self._initial_get_rows(session, url)
            calls_needed = int(math.ceil(num_found / COUNT))
            tasks = [
                asyncio.create_task(self._get_rows(i, session, url))
                for i in range(1, calls_needed + 1)
            ]
            _rows = await asyncio.gather(*tasks)
        rows = [item for sublist in _rows for item in sublist]
        return rows

    async def _initial_get_rows(self, session, url):
        async with session.get(
            url,
            params=self.params,
            headers=HEADERS,
        ) as r:
            res = await r.json()
        return res["numFound"]

    async def _get_rows(self, i, session, url):
        params = {
            **self.params,
        }
        params["page"] = i
        async with session.get(
            url,
            params=params,
            headers=HEADERS,
        ) as r:
            res = await r.json()
        return res[self.row_key]


class IncrementalStandardGetter(IncrementalGetter):
    @property
    def params(self):
        return {
            "start_time_since": self.start.strftime(TIMESTAMP_FORMAT),
            "start_time_to": self.end.strftime(TIMESTAMP_FORMAT),
            "count": COUNT,
            "order_by": "start_time",
            "order_type": "asc",
        }


class IncrementalDetailsGetter(IncrementalGetter):
    @property
    def params(self):
        return {
            "updated_since": self.start.strftime(TIMESTAMP_FORMAT),
            "updated_to": self.end.strftime(TIMESTAMP_FORMAT),
            "count": COUNT,
            "order_by": "updated_at",
            "order_type": "asc",
        }


class DetailsGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.parent = model.parent
        self.table = model.table
        self.detail_key = model.detail_key
        self.deleted_model = model.deleted_model

    def get(self):
        results, deleted = asyncio.run(self._get())
        self.deleted_model(rows=deleted).run()
        return results

    def _get_detail_ids(self):
        query = f"""
        SELECT
            s.`{self.detail_key}` AS id
        FROM
            `{DATASET}.{self.parent.capitalize()}` s
        LEFT JOIN `{DATASET}.{self.table}` d
            ON s.`{self.detail_key}` = d.`{self.detail_key}`
        WHERE
            (
                (
                    timestamp_trunc(timestamp_add(s.`updated_at`, INTERVAL 500 millisecond), SECOND)
                    <>
                    timestamp_trunc(timestamp_add(d.`updated_at`, INTERVAL 500 millisecond), SECOND)
                    AND
                    timestamp_trunc(s.`updated_at`, SECOND) 
                    <>
                    timestamp_trunc(d.`updated_at`, SECOND)
                )
                OR d.updated_at IS NULL
            )
            AND s.{self.detail_key} NOT IN (
                SELECT
                    {self.detail_key}
                FROM
                    `{DATASET}.Deleted{self.parent.capitalize()}`
            )
        LIMIT {DETAILS_LIMIT}
        """
        results = BQ_CLIENT.query(query).result()
        rows = [dict(row.items()) for row in results]
        ids = [row["id"] for row in rows]
        return ids

    async def _get(self):
        row_ids = self._get_detail_ids()
        connector = aiohttp.TCPConnector(limit=3)
        timeout = aiohttp.ClientTimeout(total=540)
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as session:
            tasks = [
                asyncio.create_task(self._get_rows(session, row_id))
                for row_id in row_ids
            ]
            rows = await asyncio.gather(*tasks)
        results_rows = [row for row in rows if row.get("deleted") is None]
        deleted_rows = [row for row in rows if row.get("deleted") is True]
        return results_rows, deleted_rows

    async def _get_rows(self, session, row_id):
        async with session.get(
            f"{BASE_URL}/{self.endpoint}/{row_id}",
            headers=HEADERS,
        ) as r:
            if r.status == 500 or r.status == 404:
                return {
                    self.detail_key: row_id,
                    "deleted": True,
                }
            else:
                res = await r.json()
                return res[self.row_key]


class DeletedGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.rows = model.rows

    def get(self):
        return self.rows
