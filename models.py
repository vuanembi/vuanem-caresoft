import os
import sys
import json
import asyncio
import math
from abc import ABCMeta, abstractmethod
from datetime import datetime
import time

import aiohttp
from google.cloud import bigquery
import jinja2

HEADERS = {
    "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
    "Content-Type": "application/json",
}
BASE_URL = "https://api.caresoft.vn/VUANEM/api/v1/"
BQ_CLIENT = bigquery.Client()
DATASET = "Caresoft"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)
COUNT = 500
CARESOFT_X_RATE_LIMIT = 5000


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class Caresoft(metaclass=ABCMeta):
    def __init__(self, table, endpoint):
        self.table = table
        self.endpoint = endpoint
        with open(f"configs/{table}.json", "r") as f:
            config = json.load(f)
        self.keys = config["keys"]
        self.schema = config["schema"]
        self.LIMIT_SWITCH = False

    @staticmethod
    def factory(table, start=None, end=None):
        if table in ["Tickets"]:
            return CaresoftIncrementalDetails(table, "ticket", "ticket_id", start, end)
        elif table in ["Contacts"]:
            return CaresoftIncrementalDetails(table, "contact", "id", start, end)
        elif table in ["Calls", "Chats"]:
            return CaresoftIncrementalStandard(table, start, end)
        elif table in ["Groups", "Agents", "Services"]:
            return CaresoftDimensions(table)
        elif table in ["TicketsCustomFields"]:
            return CaresoftCustomFields(table, "tickets/custom_fields")
        elif table in ["ContactsCustomFields"]:
            return CaresoftCustomFields(table, "contacts/custom_fields")
        else:
            raise NotImplementedError

    @abstractmethod
    def get_row_key(self):
        raise NotImplementedError

    def load(self, rows, table):
        load_target = self._fetch_load_target(table)
        write_disposition = self._fetch_write_disposition()
        loads = BQ_CLIENT.load_table_from_json(
            rows,
            load_target,
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

        return loads

        # with open(f"{table}.json", "w") as f:
            # json.dump(rows, f)

    @abstractmethod
    def _fetch_load_target(self):
        raise NotImplementedError

    @abstractmethod
    def _fetch_write_disposition(self):
        raise NotImplementedError

    def run(self):
        return asyncio.run(self._run_wrapper())

    async def _run_wrapper(self):
        connection = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connection) as session:
            return await self._run(session)

    @abstractmethod
    async def _run(self):
        raise NotImplementedError


class CaresoftStatic(Caresoft):
    def __init__(self, table, endpoint=None):
        super().__init__(table, endpoint)

    async def get_rows(self, session):
        if self.endpoint is None:
            self.endpoint = self.table.lower()

        url = BASE_URL + self.endpoint

        async with session.get(url, headers=HEADERS) as r:
            res = await r.json()

        rows = res[self.get_row_key()]
        self.num_processed = len(rows)
        return rows

    @abstractmethod
    def get_row_key(self):
        raise NotImplementedError

    def _fetch_load_target(self, table):
        return f"{DATASET}.{table}"

    def _fetch_write_disposition(self):
        return "WRITE_TRUNCATE"

    async def _run(self, session):
        rows = await self.get_rows(session)
        loads = self.load(rows, self.table)
        return [self._make_responses(loads)]

    def _make_responses(self, loads):
        return {
            "table": self.table,
            "num_processed": self.num_processed,
            "output_rows": loads.output_rows,
            "errors": loads.errors,
        }


class CaresoftDimensions(CaresoftStatic):
    def __init__(self, table):
        super().__init__(table)

    def get_row_key(self):
        return self.endpoint


class CaresoftCustomFields(CaresoftStatic):
    def __init__(self, table, endpoint):
        super().__init__(table, endpoint)

    def get_row_key(self):
        return "custom_fields"


class CaresoftIncremental(Caresoft):
    def __init__(self, table, start, end):
        self.start, self.end = self.get_time_range(start, end)
        super().__init__(table, table.lower())

    def get_time_range(self, start, end):
        if start and end:
            start, end = [
                datetime.strptime(i, DATE_FORMAT).strftime(TIMESTAMP_FORMAT)
                for i in [start, end]
            ]
        else:
            now = datetime.utcnow()
            end = now.strftime(TIMESTAMP_FORMAT)
            start = self._fetch_latest_incre()
        return start, end

    def _fetch_latest_incre(self):
        """Fetch latest incremental value

        Returns:
            str: Latest incremental Value
        """

        template = TEMPLATE_ENV.get_template("read_max_incremental.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            incremental_key=self.keys["incremental_key"],
        )
        rows = BQ_CLIENT.query(rendered_query).result()
        row = [row for row in rows][0]
        max_incre = row.get("incre")
        return max_incre.strftime(TIMESTAMP_FORMAT)

    async def get_rows(self, session):
        url = BASE_URL + self.endpoint
        params = self._make_params()

        rows = []
        num_found = await self._initial_get_rows(session, url, params)
        print(num_found)
        calls_needed = math.ceil(num_found / COUNT)
        tasks = [
            asyncio.create_task(self._get_rows(i, session, url, params))
            for i in range(1, calls_needed + 2)
        ]
        _rows = await asyncio.gather(*tasks)
        rows = [item for sublist in _rows for item in sublist]
        self.num_processed = len(rows)
        return rows

    async def _initial_get_rows(self, session, url, params):
        i = 1
        while True:
            if i < 5:
                try:
                    async with session.get(url, params=params, headers=HEADERS) as r:
                        res = await r.json()
                    break
                except aiohttp.client_exceptions.ServerDisconnectedError:
                    time.sleep(i)
                    i = i + 1
            else:
                break

        return res["numFound"]

    async def _get_rows(self, i, session, url, params):
        if not self.LIMIT_SWITCH:
            params["page"] = i
            j = 1
            while True:
                if j < 5:
                    try:
                        async with session.get(
                            url, params=params, headers=HEADERS
                        ) as r:
                            res_headers = dict(r.headers)
                            res = await r.json()
                        break
                    except aiohttp.client_exceptions.ServerDisconnectedError:
                        time.sleep(j)
                        j = j + 1
                else:
                    raise RuntimeError
            _rows = res[self.get_row_key()]
            limit_remaining = int(res_headers["X-RateLimit-Remaining"])
            if limit_remaining < CARESOFT_X_RATE_LIMIT:
                self.LIMIT_SWITCH = True
            return _rows

    @abstractmethod
    def _make_params(self):
        raise NotImplementedError

    def get_row_key(self):
        return self.endpoint

    def _fetch_load_target(self, table):
        return f"{DATASET}._stage_{table}"

    def _fetch_write_disposition(self):
        return "WRITE_APPEND"
    
    def _update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=self.keys.get('p_key'),
            incremental_key=self.keys.get("incremental_key"),
        )
        BQ_CLIENT.query(rendered_query).result()

    @abstractmethod
    def _make_responses(self, loads):
        raise NotImplementedError


class CaresoftIncrementalStandard(CaresoftIncremental):
    def __init__(self, table, start, end):
        super().__init__(table, start, end)

    def _make_params(self):
        return {
            "start_time_since": self.start,
            "start_time_to": self.end,
            "count": COUNT,
            "order_by": "start_time",
            "order_type": "asc",
        }

    async def _run(self, session):
        rows = await self.get_rows(session)
        if len(rows) > 0:
            loads = self.load(rows, self.table)
            self._update()
        else:
            loads = None
        return [self._make_responses(loads)]

    def _make_responses(self, loads_rows):
        if loads_rows is None:
            return {"table": self.table, "num_processed": self.num_processed}
        else:
            return {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": loads_rows.output_rows,
                "errors": loads_rows.errors,
            }


class CaresoftIncrementalDetails(CaresoftIncremental):
    def __init__(self, table, detail_key, detail_id, start, end):
        self.detail_key = detail_key
        self.detail_id = detail_id
        super().__init__(table, start, end)

    async def _run(self, session):
        rows = await self.get_rows(session)
        # with open('Contacts.json', 'r') as f:
        #     rows = json.load(f)
        if len(rows) > 0:
            loads = self.load(rows, self.table)
            self._update()
            rows_details = CaresoftDetails(
                f"{self.table}Details", self.detail_key, self.detail_id, rows
            )
            rows_details_responses = await rows_details._run(session)
            rows_responses = self._make_responses(loads)
            return [rows_responses, rows_details_responses]
        else:
            return [self._make_responses(None)]

    def _make_params(self):
        return {
            "updated_since": self.start,
            "updated_to": self.end,
            "count": COUNT,
            "order_by": "updated_at",
            "order_type": "asc",
        }

    def _make_responses(self, loads):
        if loads is None:
            rows_responses = {"table": self.table, "num_processed": self.num_processed}
        else:
            rows_responses = {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": loads.output_rows,
                "errors": loads.errors,
            }
        return rows_responses


class CaresoftDetails(CaresoftIncremental):
    def __init__(self, table, detail_key, detail_id, row_ids):
        self.detail_key = detail_key
        self.detail_id = detail_id
        self.row_ids = row_ids
        super().__init__(table, start=None, end=None)
        self.endpoint = self.endpoint.replace("details", "")

    def get_time_range(self, start, end):
        return start, end

    async def get_rows(self, session):
        row_ids = [row[self.detail_id] for row in self.row_ids]
        tasks = [
            asyncio.create_task(self._get_rows(session, row_id)) for row_id in row_ids
        ]
        rows = await asyncio.gather(*tasks)
        rows = [row for row in rows if row is not None]
        self.num_processed = len(rows)
        return rows

    async def _get_rows(self, session, row_id):
        if not self.LIMIT_SWITCH:
            url = BASE_URL + self.endpoint + "/" + str(row_id)
            i = 1
            while True:
                if i < 5:
                    try:
                        async with session.get(url, headers=HEADERS) as r:
                            res_headers = dict(r.headers)
                            res = await r.json()
                        break
                    except aiohttp.client_exceptions.ServerDisconnectedError:
                        time.sleep(i)
                        i = i + 1
                else:
                    break
            limit_remaining = int(res_headers["X-RateLimit-Remaining"])
            print(limit_remaining)
            if limit_remaining < CARESOFT_X_RATE_LIMIT:
                self.LIMIT_SWITCH = True
            _rows = res[self.detail_key]
            _rows
            return _rows
        else:
            return None

    def _make_params(self):
        return {}

    async def _run(self, session):
        rows = await self.get_rows(session)
        # with open('ContactsDetails.json', 'r') as f:
            # rows = json.load(f)
        if len(rows) > 0:
            loads = self.load(rows, self.table)
            self._update()
        else:
            loads = None
        return self._make_responses(loads)

    def _make_responses(self, loads_rows):
        if loads_rows is None:
            return {"table": self.table, "num_processed": self.num_processed}
        else:
            return {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": loads_rows.output_rows,
                "errors": loads_rows.errors,
            }
