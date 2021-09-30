import sys
import asyncio
import math
from abc import ABCMeta, abstractmethod
from datetime import datetime

import requests
import aiohttp
from google.api_core.exceptions import NotFound

from components.loader import BigQueryAppendLoader, PostgresLoader
from components.utils import (
    BASE_URL,
    HEADERS,
    COUNT,
    TEMPLATE_ENV,
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
                template = TEMPLATE_ENV.get_template("read_max_incremental.sql.j2")
                rendered_query = template.render(
                    dataset=DATASET,
                    table=self.table,
                    incre_key=self.keys.get("incre_key"),
                )
                rows = BQ_CLIENT.query(rendered_query).result()
                row = [row for row in rows][0]
                max_incre = row["incre"]
            except (KeyError, NotFound):
                max_incre = datetime(2020, 6, 1)
            start = max_incre
        return start, end

    async def _get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        connector = aiohttp.TCPConnector(limit=50)
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
        self.parent = model.parent
        self.table = model.table
        self.detail_key = model.detail_key
        self.loader = [
            BigQueryAppendLoader(f"Deleted{self.parent.capitalize()}"),
            PostgresLoader(model.deleted_model),
        ]

    def get(self):
        return asyncio.run(self._get())

    def _get_detail_ids(self):
        template = TEMPLATE_ENV.get_template("read_detail_ids.sql.j2")
        with_ref = True
        attempts = 0
        while attempts < 2:
            try:
                query = template.render(
                    dataset=DATASET,
                    parent=self.parent.capitalize(),
                    table=self.table,
                    detail_id=self.detail_key,
                    limit=DETAILS_LIMIT,
                    with_ref=with_ref,
                )
                results = BQ_CLIENT.query(query).result()
                break
            except NotFound:
                with_ref = False
                attempts += 1
        rows = [dict(row.items()) for row in results]
        ids = [row["id"] for row in rows]
        return ids

    async def _get(self):
        row_ids = self._get_detail_ids()
        connector = aiohttp.TCPConnector(limit=50)
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
        [loader.load(deleted_rows) for loader in self.loader]
        return results_rows

    async def _get_rows(self, session, row_id):
        async with session.get(
            f"{BASE_URL}/{self.endpoint}/{row_id}",
            headers=HEADERS,
        ) as r:
            if r.status == 500:
                return {
                    self.detail_key: row_id,
                    "deleted": True,
                }
            else:
                res = await r.json()
                return res[self.row_key]
