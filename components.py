import os
import sys
import json
import asyncio
import math
from abc import ABCMeta, abstractmethod
from datetime import datetime

import requests
import aiohttp
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import jinja2
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert


from pg_models import Base


# API Headers
HEADERS = {
    "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
    "Content-Type": "application/json",
}
BASE_URL = "https://api.caresoft.vn/VUANEM/api/v1"

# API Calls Configs
COUNT = 500
DETAILS_LIMIT = 10

# BigQuery Configs
BQ_CLIENT = bigquery.Client()
DATASET = "Caresoft2"

# Datetime Formatting
NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Jinja2 Configs
TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

ENGINE = create_engine(
    "postgresql+psycopg2://"
    + f"{os.getenv('PG_UID')}:{os.getenv('PG_PWD')}@"
    + f"{os.getenv('PG_HOST')}/{os.getenv('PG_DB')}",
    echo=True,
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class Caresoft(metaclass=ABCMeta):
    def __init__(self):
        self.keys, self.schema = self.get_config()

    @property
    @abstractmethod
    def table(self):
        pass

    @property
    @abstractmethod
    def endpoint(self):
        pass

    @property
    @abstractmethod
    def row_key(self):
        pass

    @abstractmethod
    def transform(self, rows):
        pass

    def get_config(self):
        """Get config from json

        Returns:
            tuple: keys, schema
        """

        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    def run(self):
        rows = self.getter.get()
        response = {
            "table": self.table,
            "num_processed": len(rows),
        }
        if getattr(self, "start", None) and getattr(self, "end", None):
            response["start"] = self.start.strftime(TIMESTAMP_FORMAT)
            response["end"] = self.end.strftime(TIMESTAMP_FORMAT)
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = [loader.load(rows) for loader in self.loader]
            response["loads"] = loads
        return response


class Getter(metaclass=ABCMeta):
    def __init__(self, endpoint, row_key):
        self.endpoint = endpoint
        self.row_key = row_key

    @abstractmethod
    def get(self):
        pass


class SimpleGetter(Getter):
    def __init__(self, endpoint, row_key):
        super().__init__(endpoint, row_key)

    def get(self):
        url = f"{BASE_URL}/{self.endpoint}"
        with requests.get(url, headers=HEADERS) as r:
            res = r.json()
        rows = res[self.row_key]
        return rows


class IncrementalGetter(Getter):
    def __init__(self, params, endpoint, row_key):
        super().__init__(endpoint, row_key)
        self.params = params

    def get(self):
        return asyncio.run(self._get())

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
        async with session.get(url, params=self.params, headers=HEADERS) as r:
            res = await r.json()
        return res["numFound"]

    async def _get_rows(self, i, session, url):
        params = {**self.params}
        params["page"] = i
        async with session.get(url, params=params, headers=HEADERS) as r:
            res = await r.json()
        _rows = res[self.row_key]
        return _rows


class DetailsGetter(Getter):
    def __init__(self, parent, table, detail_key, endpoint, row_key, deleted_model):
        super().__init__(endpoint, row_key)
        self.parent = parent
        self.table = table
        self.detail_key = detail_key
        self.loader = [
            BigQueryAppendLoader(f"Deleted{self.parent.capitalize()}"),
            PostgresLoader(deleted_model),
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
        url = f"{BASE_URL}/{self.endpoint}/{row_id}"
        async with session.get(
            url,
            headers=HEADERS,
        ) as r:
            if r.status == 500:
                _rows = {
                    self.detail_key: row_id,
                    "deleted": True,
                }
            else:
                res = await r.json()
                _rows = res[self.row_key]
        return _rows


class Loader(metaclass=ABCMeta):
    def __init__(self):
        pass

    @abstractmethod
    def load(self, rows):
        pass


class BigQueryLoader(Loader):
    def __init__(self, table):
        self.table = table

    @property
    @abstractmethod
    def load_target(self):
        pass

    @property
    @abstractmethod
    def write_disposition(self):
        pass

    @property
    def config(self):
        """Get config from json

        Returns:
            tuple: keys, schema
        """

        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config

    def _load(self, rows):
        loads = BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}.{self.load_target}",
            job_config=bigquery.LoadJobConfig(
                schema=self.config["schema"],
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=self.write_disposition,
            ),
        ).result()
        return {
            "load": "BigQuery",
            "output_rows": loads.output_rows,
        }


class BigQuerySimpleLoader(BigQueryLoader):
    @property
    def load_target(self):
        return f"{self.table}"

    write_disposition = "WRITE_TRUNCATE"

    def load(self, rows):
        loads = self._load(rows)
        return loads


class BigQueryIncrementalLoader(BigQueryLoader):
    @property
    def load_target(self):
        return f"_stage_{self.table}"

    write_disposition = "WRITE_APPEND"

    def load(self, rows):
        loads = self._load(rows)
        self._update()
        return loads

    def _update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.config["keys"].get("p_key")),
            incre_key=self.config["keys"].get("incre_key"),
        )
        BQ_CLIENT.query(rendered_query).result()


class BigQueryAppendLoader(BigQueryLoader):
    @property
    def load_target(self):
        return f"_stage_{self.table}"

    write_disposition = "WRITE_APPEND"

    def load(self, rows):
        loads = self._load(rows)
        return loads


class PostgresLoader(Loader):
    def __init__(self, model):
        super().__init__()
        self.model = model

    def load(self, rows):
        Session = sessionmaker(bind=ENGINE)
        Base.metadata.create_all(ENGINE)
        table = self.model.__table__
        update_cols = [
            c.name for c in table.c if c not in list(table.primary_key.columns)
        ]
        stmt = insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns,
            set_={k: getattr(stmt.excluded, k) for k in update_cols},
        )
        with Session() as session:
            loads = session.execute(stmt)
            session.commit()
        return {
            "load": "Postgres",
            "output_rows": loads.rowcount,
        }


class CaresoftStatic(Caresoft):
    def __init__(self):
        self.getter = SimpleGetter(self.endpoint, self.row_key)
        self.loader = [
            BigQuerySimpleLoader(self.table),
            PostgresLoader(self.model),
        ]


class CaresoftIncremental(Caresoft):
    def __init__(self, start, end):
        super().__init__()
        self.start, self.end = self._get_time_range(start, end)
        self.getter = IncrementalGetter(self.params, self.endpoint, self.row_key)
        self.loader = [
            BigQueryIncrementalLoader(self.table),
            PostgresLoader(self.model),
        ]

    @property
    @abstractmethod
    def params(self):
        pass

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


class CaresoftIncrementalStandard(CaresoftIncremental):
    @property
    def params(self):
        return {
            "start_time_since": self.start.strftime(TIMESTAMP_FORMAT),
            "start_time_to": self.end.strftime(TIMESTAMP_FORMAT),
            "count": COUNT,
            "order_by": "start_time",
            "order_type": "asc",
        }

    def __init__(self, start, end):
        super().__init__(start, end)


class CaresoftIncrementalDetails(CaresoftIncremental):
    @property
    def params(self):
        return {
            "updated_since": self.start.strftime(TIMESTAMP_FORMAT),
            "updated_to": self.end.strftime(TIMESTAMP_FORMAT),
            "count": COUNT,
            "order_by": "updated_at",
            "order_type": "asc",
        }

    def __init__(self, start, end):
        super().__init__(start, end)


class CaresoftDetails(Caresoft):
    def __init__(self):
        self.getter = DetailsGetter(
            self.parent,
            self.table,
            self.detail_key,
            self.endpoint,
            self.row_key,
            self.deleted_model,
        )
        self.loader = [
            BigQueryIncrementalLoader(self.table),
            PostgresLoader(self.model),
        ]

    @property
    @abstractmethod
    def parent(self):
        pass

    @property
    @abstractmethod
    def detail_key(self):
        pass
