from typing import Optional
from datetime import datetime

from google.cloud import bigquery

DATASET = "IP_Caresoft"
client = bigquery.Client()


def get_last_timestamp(table: str, cursor_key: str) -> datetime:
    rows = client.query(
        f"SELECT MAX({cursor_key}) AS incre FROM {DATASET}.{table}"
    ).result()
    return [row for row in rows][0]["incre"]


def load(
    table: str,
    schema: list[dict],
    id_key: Optional[str],
    cursor_key: Optional[str],
    rows: list[dict],
) -> int:
    output_rows = (
        client.load_table_from_json(
            rows,
            f"{DATASET}.{table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND" if id_key else "WRITE_TRUNCATE",
            ),
        )
        .result()
        .output_rows
    )

    if id_key and cursor_key:
        _update(table, id_key, cursor_key)

    return output_rows


def _update(table: str, id_key: str, cursor_key: str) -> None:
    client.query(
        f"""
        CREATE OR REPLACE TABLE {DATASET}.{table} AS
        SELECT * EXCEPT (row_num)
        FROM
        (
            SELECT
                *,
                ROW_NUMBER() over (
                    PARTITION BY {id_key}
                    ORDER BY {cursor_key} DESC
                ) AS row_num
            FROM {DATASET}.{table}
        ) WHERE row_num = 1
        """
    ).result()
