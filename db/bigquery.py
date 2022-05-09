from typing import Optional
from datetime import datetime

from google.cloud import bigquery

DATASET = "Caresoft"
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
    partition_key: Optional[str],
    rows: list[dict],
) -> int:
    _table = table if not partition_key else f"p_{table}"
    output_rows = (
        client.load_table_from_json(
            rows,
            f"{DATASET}.{_table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND" if id_key else "WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_="DAY",
                    field=partition_key,
                )
                if partition_key
                else None,
            ),
        )
        .result()
        .output_rows
    )

    return output_rows
