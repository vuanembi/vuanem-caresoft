import os
import sys
import json
import asyncio
import math
from abc import ABCMeta, abstractmethod
from datetime import datetime

import aiohttp
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import jinja2

# API Headers
HEADERS = {
    "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
    "Content-Type": "application/json",
}
BASE_URL = "https://api.caresoft.vn/VUANEM/api/v1/"

# BigQuery Configs
BQ_CLIENT = bigquery.Client()
DATASET = "Caresoft"

# Datetime Formatting
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Jinja2 Configs
TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

# API Calls Configs
COUNT = 500
CARESOFT_X_RATE_LIMIT = 5000
STANDARD_LIMIT = 50000
DETAILS_LIMIT = 2500

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class Caresoft(metaclass=ABCMeta):
    def __init__(self, table):
        """Init Pipelines

        Args:
            table (str): Table name
        """

        self.table = table
        self.endpoint = self.get_endpoint()
        self.keys, self.schema = self.get_config()

    @staticmethod
    def factory(table, start=None, end=None):
        """Factory Method to create pipelines

        Args:
            table (str): Table name
            start (str, optional): Date in %Y-%m-%d. Defaults to None.
            end (str, optional): Date in %Y-%m-%d. Defaults to None.

        Raises:
            NotImplementedError: No pipelines is matched

        Returns:
            Caresoft: Pipelines
        """

        if table in ["Tickets"]:
            return CaresoftIncrementalDetails(table, start, end)
        elif table in ["Contacts"]:
            return CaresoftIncrementalDetails(table, start, end)
        elif table in ["Calls", "Chats"]:
            return CaresoftIncrementalStandard(table, start, end)
        elif table in ["Groups", "Agents", "Services"]:
            return CaresoftDimensions(table)
        elif table in ["TicketsDetails"]:
            return CaresoftDetails(table, "tickets", "ticket_id", "ticket")
        elif table in ["ContactsDetails"]:
            return CaresoftDetails(table, "contacts", "id", "contact")
        elif table in ["TicketsCustomFields"]:
            return CaresoftCustomFields(table, "tickets")
        elif table in ["ContactsCustomFields"]:
            return CaresoftCustomFields(table, "contacts")
        else:
            raise NotImplementedError

    def get_config(self):
        """Get config from json

        Returns:
            tuple: keys, schema
        """

        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    @abstractmethod
    def get_endpoint(self):
        """Abstract Method to get API endpoint

        Returns:
            str: API endpoint
        """

        raise NotImplementedError

    @abstractmethod
    def get_row_key(self):
        """Abstract Method row key

        Returns:
            str: Row key
        """

        raise NotImplementedError

    @abstractmethod
    def transform(self, rows):
        """Abstract Method to transform rows

        Args:
            rows (list): List of dicts

        Returns:
            list: List of dicts
        """

        raise NotImplementedError

    def load(self, rows, table):
        """Load rows to stage table on BigQuery

        Args:
            rows (list): List of dicts
            table (str): Table name

        Returns:
            google.cloud.bigquery.job.base_AsyncJob: LoadJob Results
        """

        load_target = self._get_load_target(table)
        write_disposition = self._get_write_disposition()
        loads = BQ_CLIENT.load_table_from_json(
            rows,
            load_target,
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
                ignore_unknown_values=True,
            ),
        ).result()
        return loads

    @abstractmethod
    def _get_load_target(self):
        """Get load target

        Returns:
            str: Load table
        """

        raise NotImplementedError

    @abstractmethod
    def _get_write_disposition(self):
        """Get write_disposition

        Returns:
            str: Load table
        """

        raise NotImplementedError

    def run(self):
        """Run Job

        Returns:
            dict: Job Results
        """

        return asyncio.run(self._run_wrapper())

    async def _run_wrapper(self):
        """Initialize aiohttp Client for running jobs

        Returns:
            dict: Job Results
        """

        CONNECTOR = aiohttp.TCPConnector(limit=20)
        TIMEOUT = aiohttp.ClientTimeout(total=599)
        async with aiohttp.ClientSession(
            connector=CONNECTOR, timeout=TIMEOUT
        ) as session:
            return await self._run(session)

    @abstractmethod
    async def _run(self):
        """Run Async Job

        Returns:
            dict: Job Results
        """

        raise NotImplementedError

    def _make_responses(self, rows, loads):
        """Make responses

        Args:
            loads (google.cloud.bigquery.job.base_AsyncJob): LoadJob Results

        Returns:
            dict: Job Results
        """
        rows_responses = {
            "table": self.table,
            "start": getattr(self, "start", None),
            "end": getattr(self, "end", None),
            "num_processed": len(rows),
        }
        if loads:
            rows_responses = {
                **rows_responses,
                "output_rows": loads.output_rows,
                "errors": loads.errors,
            }
        return rows_responses


class CaresoftStatic(Caresoft):
    def __init__(self, table):
        super().__init__(table)

    async def get_rows(self, session):
        """Get rows

        Args:
            session (aiohttp.ClientSession): HTTP Client

        Returns:
            list: List of dicts
        """

        url = BASE_URL + self.endpoint

        async with session.get(url, headers=HEADERS) as r:
            res = await r.json()

        rows = res[self.get_row_key()]
        return rows

    @abstractmethod
    def get_row_key(self):
        raise NotImplementedError

    def transform(self, rows):
        """No transform is needed

        Args:
            rows (list): List of dicts

        Returns:
            list: List of dicts
        """

        return rows

    def _get_load_target(self, table):
        return f"{DATASET}.{table}"

    def _get_write_disposition(self):
        return "WRITE_TRUNCATE"

    async def _run(self, session):
        rows = await self.get_rows(session)
        rows = self.transform(rows)
        loads = self.load(rows, self.table)
        return [self._make_responses(rows, loads)]

class CaresoftDimensions(CaresoftStatic):
    def __init__(self, table):
        super().__init__(table)

    def get_endpoint(self):
        return self.table.lower()

    def get_row_key(self):
        return self.table.lower()


class CaresoftCustomFields(CaresoftStatic):
    def __init__(self, table, parent):
        self.parent = parent
        super().__init__(table)

    def get_config(self):
        with open(f"configs/{self.parent.capitalize()}CustomFields.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    def get_endpoint(self):
        return f"{self.parent}/custom_fields"

    def get_row_key(self):
        return "custom_fields"


class CaresoftIncremental(Caresoft):
    def __init__(self, table, start, end):
        super().__init__(table)
        self.start, self.end = self.get_time_range(start, end)

    def get_endpoint(self):
        return self.table.lower()

    def get_time_range(self, start, end):
        """Get Time range to run, if start & end is None then read the max incremental from BigQuery

        Args:
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d

        Returns:
            tuple: (start, end)
        """

        if start and end:
            start, end = [
                datetime.strptime(i, DATE_FORMAT).strftime(TIMESTAMP_FORMAT)
                for i in [start, end]
            ]
        else:
            now = datetime.utcnow()
            end = now.strftime(TIMESTAMP_FORMAT)
            start = self._get_latest_incre()
        return start, end

    def _get_latest_incre(self):
        """Fetch latest incremental value

        Returns:
            str: Latest incremental Value
        """

        try:
            template = TEMPLATE_ENV.get_template("read_max_incremental.sql.j2")
            rendered_query = template.render(
                dataset=DATASET,
                table=self.table,
                incremental_key=self.keys["incremental_key"],
            )
            rows = BQ_CLIENT.query(rendered_query).result()
            row = [row for row in rows][0]
            max_incre = row["incre"]
        except (KeyError, NotFound):
            max_incre = datetime(2020, 6, 1)
        return max_incre.strftime(TIMESTAMP_FORMAT)

    async def get_rows(self, session):
        """Get rows

        Args:
            session (aiohttp.ClientSession): HTTP Client

        Returns:
            list: List of dicts
        """

        url = BASE_URL + self.endpoint
        params = self._make_params()

        num_found = await self._initial_get_rows(session, url, params)
        print(num_found)
        calls_needed = math.ceil(num_found / COUNT)
        calls = min([int(calls_needed), int(STANDARD_LIMIT / COUNT)])
        tasks = [
            asyncio.create_task(self._get_rows(i, session, url, params))
            for i in range(1, calls + 1)
        ]
        _rows = await asyncio.gather(*tasks)
        rows = [item for sublist in _rows for item in sublist]
        return rows

    async def _initial_get_rows(self, session, url, params):
        """Get the first results batch to check how many calls is needed

        Args:
            session (aiohttp.ClientSession): HTTP Client
            url (str): URL
            params (dict): Parameters

        Returns:
            int: NumFound
        """

        async with session.get(url, params=params, headers=HEADERS) as r:
            res = await r.json()
        return res["numFound"]

    async def _get_rows(self, i, session, url, params):
        """Get individual page of results

        Args:
            i (int): Page
            session (aiohttp.ClientSession): HTTP Client
            url (str): URL
            params (dict): Parameters

        Returns:
            dict: Row
        """

        params["page"] = i
        async with session.get(url, params=params, headers=HEADERS) as r:
            res = await r.json()
        _rows = res[self.get_row_key()]
        return _rows

    def transform(self, rows):
        """Transform rows

        Args:
            rows (list): List of dicts

        Returns:
            list: List of dicts
        """

        _rows = []
        for row in rows:
            _row = {
                "value": json.dumps(row),
            }
            for i in self.keys.get("incremental_key"):
                _row[i] = row[i]
            for i in self.keys.get("p_key"):
                _row[i] = row[i]
            _rows.append(_row)
        return _rows

    @abstractmethod
    def _make_params(self):
        """Abstract Method to make parameters for calls

        Returns:
            dict: Parameters
        """

        raise NotImplementedError

    def get_row_key(self):
        return self.endpoint

    def _get_load_target(self, table):
        return f"{DATASET}._stage_{table}"

    def _get_write_disposition(self):
        return "WRITE_APPEND"

    async def _run(self, session):
        """Async Run Job

        Args:
            session (aiohttp.ClientSession): HTTP Client

        Returns:
            dict: Job Results
        """

        rows = await self.get_rows(session)
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows, self.table)
            self._update()
        else:
            loads = None
        return [self._make_responses(rows, loads)]

    def _update(self):
        """Update rows from stage to raw to main table"""

        self._update_from_stage()
        self._update_from_raw()

    def _update_from_stage(self):
        """Update rows from stage to raw"""

        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=self.keys.get("p_key"),
            incremental_key=self.keys.get("incremental_key"),
        )
        BQ_CLIENT.query(rendered_query).result()

    def _update_from_raw(self):
        """Update rows from raw to main"""

        template = TEMPLATE_ENV.get_template("update_from_raw.sql.j2")
        rendered_query = template.render(dataset=DATASET, table=self.table)
        BQ_CLIENT.query(rendered_query)


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


class CaresoftIncrementalDetails(CaresoftIncremental):
    def __init__(self, table, start, end):
        super().__init__(table, start, end)

    def _make_params(self):
        return {
            "updated_since": self.start,
            "updated_to": self.end,
            "count": COUNT,
            "order_by": "updated_at",
            "order_type": "asc",
        }


class CaresoftDetails(CaresoftIncremental):
    def __init__(self, table, parent, detail_id, detail_key):
        """Initialize Details Pipelines

        Args:
            table (str): Table name
            parent (str): Parent name
            detail_id (str): Detail ID
            detail_key (str): Detail Key
        """

        self.parent = parent
        self.detail_id = detail_id
        self.detail_key = detail_key
        super().__init__(table, start=None, end=None)

    def get_config(self):
        with open(f"configs/{self.parent.capitalize()}Details.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    def get_endpoint(self):
        return self.parent

    def get_time_range(self, start, end):
        return start, end

    def get_detail_ids(self):
        """Get misaligning ids in details table vs. main table

        Returns:
            list: List of ids
        """

        template = TEMPLATE_ENV.get_template("read_detail_ids.sql.j2")
        query = template.render(
            dataset=DATASET,
            parent=self.parent.capitalize(),
            table=self.table,
            detail_id=self.detail_id,
            limit=DETAILS_LIMIT,
        )
        results = BQ_CLIENT.query(query).result()
        rows = [dict(row.items()) for row in results]
        ids = [row["id"] for row in rows]
        return ids

    def _make_params(self):
        """No Paramters

        Returns:
            dict: Parameters
        """

        return {}

    async def get_rows(self, session):
        """Get rows

        Args:
            session (aiohttp.ClientSession): HTTP Client

        Returns:
            list: List of dicts
        """

        row_ids = self.get_detail_ids()
        tasks = [
            asyncio.create_task(self._get_rows(session, row_id)) for row_id in row_ids
        ]
        rows = await asyncio.gather(*tasks)
        results_rows = [row for row in rows if row.get("deleted") is None]
        deleted_rows = [row for row in rows if row.get("deleted") is True]
        self.load(deleted_rows, f"Deleted{self.parent.capitalize()}")
        return results_rows

    async def _get_rows(self, session, row_id):
        """Get individual row

        Args:
            session (aiohttp.ClientSession): HTTP Client
            row_id (str): Row ID

        Returns:
            dict: Row
        """

        url = BASE_URL + self.endpoint + "/" + str(row_id)
        async with session.get(url, headers=HEADERS) as r:
            if r.status == 500:
                _rows = {self.detail_id: row_id, "deleted": True}
            else:
                res = await r.json()
                _rows = res[self.detail_key]
        return _rows
