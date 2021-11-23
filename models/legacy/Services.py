from sqlalchemy import Column, Integer, String

from models.models import Caresoft
from components.getter import SimpleGetter
from components.loader import BigQuerySimpleLoader, PostgresStandardLoader


class Services(Caresoft):
    getter = SimpleGetter
    loader = [
        PostgresStandardLoader,
        BigQuerySimpleLoader,
    ]
    endpoint = row_key = "services"
    schema = [
        {"name": "service_id", "type": "INTEGER"},
        {"name": "service_name", "type": "STRING"},
        {"name": "service_type", "type": "STRING"},
    ]
    columns = [
        Column("service_id", Integer, primary_key=True),
        Column("service_name", String),
        Column("service_type", String),
    ]

    def transform(self, rows):
        return [
            {
                "service_id": row.get("service_id"),
                "service_name": row.get("service_name"),
                "service_type": row.get("service_type"),
            }
            for row in rows
        ]
