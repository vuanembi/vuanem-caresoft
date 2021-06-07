import os
import sys
import json
import asyncio
import itertools
from abc import ABCMeta, abstractmethod

from tqdm import tqdm
import aiohttp
from google.cloud import bigquery

HEADERS = {
    "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
    "Content-Type": "application/json",
}
BASE_URL = "https://api.caresoft.vn/VUANEM/api/v1/"
BQ_CLIENT = bigquery.Client()
DATASET = "Caresoft"

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class Caresoft(metaclass=ABCMeta):
    def __init__(self, table, endpoint):
        self.table = table
        self.endpoint = endpoint

    @staticmethod
    def factory(table, start=None, end=None):
        if table in ["Tickets"]:
            return CaresoftIncrementalDetails(table, "ticket", "ticket_id", start, end)
        elif table in ["Contacts"]:
            return CaresoftIncrementalDetails(table, "contact", "id", start, end)
        elif table in ["Calls", "Chats"]:
            return CaresoftIncrementalStandardConversations(table, start, end)
        elif table in ["Groups", "Agents", "Services"]:
            return CaresoftDimensions(table)
        elif table in ["TicketsCustomFields"]:
            return CaresoftCustomFields(table, "tickets/custom_fields")
        elif table in ["ContactsCustomFields"]:
            return CaresoftCustomFields(table, "contacts/custom_fields")

    @abstractmethod
    def get_row_key(self):
        raise NotImplementedError

    def load(self, rows, table):
        with open(f"schemas/{table}.json", "r") as f:
            schema = json.load(f)

        load_target = self._fetch_load_target(table)
        write_disposition = self._fetch_write_disposition()
        loads = BQ_CLIENT.load_table_from_json(
            rows,
            load_target,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

        return loads

        # with open(f"{table}.json", "w") as f:
        #     json.dump(rows, f)

    @abstractmethod
    def _fetch_load_target(self):
        raise NotImplementedError

    @abstractmethod
    def _fetch_write_disposition(self):
        raise NotImplementedError

    def run(self):
        return asyncio.run(self._run_wrapper())

    async def _run_wrapper(self):
        async with aiohttp.ClientSession() as session:
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
        return [
            {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": loads.output_rows,
                "errors": loads.errors,
            }
        ]


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
        self.start = start
        self.end = end
        super().__init__(table, table.lower())

    def fetch_latest_date(self):
        pass

    async def get_rows(self, session):
        url = BASE_URL + self.endpoint
        params = self._make_params()

        rows = []
        i = 1
        for i in itertools.count(i):
            params["page"] = i
            async with session.get(url, params=params, headers=HEADERS) as r:
                res = await r.json()
            _rows = res[self.get_row_key()]
            if len(_rows) > 0:
                rows.extend(_rows)
                print(len(rows))
                i += 1
            else:
                break
        self.num_processed = len(rows)
        return rows

    @abstractmethod
    def _make_params(self):
        raise NotImplementedError

    @abstractmethod
    async def get_row_details(self, session, rows):
        raise NotImplementedError

    def get_row_key(self):
        return self.endpoint

    def _fetch_load_target(self, table):
        return f"{DATASET}._stage_{table}"

    def _fetch_write_disposition(self):
        return "WRITE_APPEND"

    async def _run(self, session):
        rows = await self.get_rows(session)
        row_details = await self.get_row_details(session, rows)
        if len(rows) > 0:
            loads_rows = self.load(rows, self.table)
            if len(row_details):
                loads_details = self.load(row_details, f"{self.table}Details")
                return self._make_responses(loads_rows, loads_details)
            return self._make_responses(loads_rows)
        else:
            return [
                {"table": self.table, "num_processed": self.num_processed},
            ]

    @abstractmethod
    def _make_responses(self, loads_rows, loads_details):
        raise NotImplementedError


class CaresoftIncrementalStandard(CaresoftIncremental):
    def __init__(self, table, start, end):
        super().__init__(table, start, end)

    async def get_row_details(self, session, rows):
        return []

    def _make_responses(self, loads_rows, loads_details):
        return [
            {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": loads_rows.output_rows,
                "errors": loads_rows.errors,
            }
        ]



class CaresoftIncrementalStandardConversations(CaresoftIncrementalStandard):
    def __init__(self, table, start, end):
        super().__init__(table, start, end)

    def _make_params(self):
        return {
            "start_time_since": "2021-06-01T00:00:00Z",
            "start_time_to": "2021-06-01T03:00:00Z",
            "count": 500,
        }


class CaresoftIncrementalDetails(CaresoftIncremental):
    def __init__(self, table, detail_key, detail_id, start, end):
        self.detail_key = detail_key
        self.detail_id = detail_id
        super().__init__(table, start, end)

    def _make_params(self):
        return {
            "updated_since": "2021-06-01T00:00:00Z",
            "updated_to": "2021-06-01T03:00:00Z",
            "count": 500,
        }

    async def get_row_details(self, session, rows):
        row_ids = [row[self.detail_id] for row in rows]
        tasks = [
            asyncio.create_task(self._get_row_details(session, row_id))
            for row_id in row_ids
        ]
        row_details = [
            await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))
        ]
        self.num_processed_details = len(row_details)
        return row_details

    async def _get_row_details(self, session, row_id):
        url = BASE_URL + self.endpoint + "/" + str(row_id)

        async with session.get(url, headers=HEADERS) as r:
            res = await r.json()
        row_details = res[self.detail_key]
        return row_details

    def _make_responses(self, loads_rows, loads_details):
        return [
            {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": loads_rows.output_rows,
                "errors": loads_rows.errors,
            },
            {
                "table": self.table,
                "num_processed": self.num_processed_details,
                "output_rows": loads_details.output_rows,
                "errors": loads_details.errors,
            },
        ]
