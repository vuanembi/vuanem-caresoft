from sqlalchemy import Column, Integer, String, DateTime

from models.models import CaresoftStatic
from components.getter import SimpleGetter
from components.loader import BigQuerySimpleLoader, PostgresStandardLoader


class Groups(CaresoftStatic):
    getter = SimpleGetter
    loader = [
        BigQuerySimpleLoader,
        PostgresStandardLoader,
    ]
    endpoint = row_key = "groups"
    schema = [
        {"name": "group_id", "type": "INTEGER"},
        {"name": "group_name", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ]
    columns = [
        Column("group_id", Integer, primary_key=True, index=True),
        Column("group_name", String),
        Column("created_at", DateTime(timezone=True)),
    ]

    def transform(self, rows):
        return [
            {
                "group_id": row.get("group_id"),
                "group_name": row.get("group_name"),
                "created_at": row.get("created_at"),
            }
            for row in rows
        ]
