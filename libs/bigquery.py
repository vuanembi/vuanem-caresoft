from typing import Callable, Optional
from datetime import datetime

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

DATE_FORMAT = "%Y-%m-%d"


def get_time_range(
    dataset: str,
    table: str,
    incre_key: dict,
    start: Optional[str],
    end: Optional[str],
) -> tuple[datetime, datetime]:
    if start and end:
        _start, _end = [datetime.strptime(i, DATE_FORMAT) for i in [start, end]]
    else:
        _end = datetime.utcnow()
        rows = BQ_CLIENT.query(
            f"""SELECT MAX({incre_key}) AS incre FROM {dataset}.{table}"""
        ).result()
        _start = [row for row in rows][0]["incre"]
    return _start, _end


def load(write_disposition: str) -> Callable[[str, str, list[dict], list[dict]], int]:
    def _load(dataset: str, table: str, schema: list[dict], rows: list[dict]) -> int:
        return (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{dataset}.{table}",
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition=write_disposition,
                ),
            )
            .result()
            .output_rows
        )

    return _load


def update(dataset: str, table: str, keys: dict) -> None:
    BQ_CLIENT.query(
        f"""
        CREATE OR REPLACE TABLE {dataset}.{table} AS
        SELECT * EXCEPT (row_num)
        FROM
        (
            SELECT
                *,
                ROW_NUMBER() over (
                    PARTITION BY {keys['p_key']}
                    ORDER BY {keys['incre_key']} DESC
                ) AS row_num
            FROM {dataset}.{table}
        ) WHERE row_num = 1
        """
    ).result()


load_truncate = load("WRITE_TRUNCATE")
load_append = load("WRITE_APPEND")


def load_simple(dataset: str, table: str, schema: list[dict], rows: list[dict]) -> int:
    return load_truncate(dataset, table, schema, rows)


def load_incremental(
    dataset: str,
    table: str,
    schema: list[dict],
    keys: dict,
    rows: list[dict],
) -> int:
    output_rows = load_append(dataset, table, schema, rows)
    update(dataset, table, keys)
    return output_rows
