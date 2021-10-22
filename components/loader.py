from abc import ABCMeta, abstractmethod

from google.cloud import bigquery
from sqlalchemy import delete, and_, insert

from config import BQ_CLIENT, DATASET, ENGINE

class Loader(metaclass=ABCMeta):
    @abstractmethod
    def load(self, rows):
        pass


class BigQueryLoader(Loader):
    def __init__(self, model):
        self.table = model.table
        self.schema = model.schema

    @property
    @abstractmethod
    def write_disposition(self):
        pass

    def _load(self, rows):
        output_rows = BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}.{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=self.write_disposition,
            ),
        ).result().output_rows
        return {
            "load": "BigQuery",
            "output_rows": output_rows,
        }


class BigQuerySimpleLoader(BigQueryLoader):
    write_disposition = "WRITE_TRUNCATE"

    def load(self, rows):
        loads = self._load(rows)
        return loads


class BigQueryIncrementalLoader(BigQueryLoader):
    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys

    write_disposition = "WRITE_APPEND"

    def load(self, rows):
        loads = self._load(rows)
        self._update()
        return loads

    def _update(self):
        query = f"""
        CREATE OR REPLACE TABLE `{DATASET}`.`{self.table}` AS
        SELECT * EXCEPT (`row_num`)
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() over (
                        PARTITION BY {','.join(self.keys['p_key'])}
                        ORDER BY {self.keys['incre_key']} DESC
                    ) AS `row_num`
                FROM
                    `{DATASET}`.`{self.table}`
            )
        WHERE
            `row_num` = 1
        """
        BQ_CLIENT.query(query).result()


class BigQueryAppendLoader(BigQueryLoader):
    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys

    write_disposition = "WRITE_APPEND"

    def load(self, rows):
        loads = self._load(rows)
        return loads


class PostgresLoader(Loader):
    def __init__(self, model):
        self.model = model.model

    def load(self, rows):
        with ENGINE.connect().execution_options(autocommit=True) as conn:
            loads = self._load(conn, rows)
        return {
            "load": "Postgres",
            "output_rows": len(loads.inserted_primary_key_rows),
        }

    @abstractmethod
    def _load(self, conn, rows):
        pass


class PostgresStandardLoader(PostgresLoader):
    def _load(self, conn, rows):
        self.model.create(bind=ENGINE, checkfirst=True)
        truncate_stmt = f'TRUNCATE TABLE "{self.model.schema}"."{self.model.name}"'
        conn.execute(truncate_stmt)
        loads = conn.execute(insert(self.model), rows)
        return loads


class PostgresIncrementalLoader(PostgresLoader):
    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys

    def _load(self, conn, rows):
        self.model.create(bind=ENGINE, checkfirst=True)
        delete_stmt = delete(self.model).where(
            and_(
                *[
                    self.model.c[p_key].in_([row[p_key] for row in rows])
                    for p_key in self.keys["p_key"]
                ]
            )
        )
        conn.execute(delete_stmt)
        loads = conn.execute(insert(self.model), rows)
        return loads
