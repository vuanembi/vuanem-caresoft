from sqlalchemy import Column, Integer, String, DateTime

from models.models import Caresoft
from components.getter import IncrementalDetailsGetter
from components.loader import BigQueryIncrementalLoader, PostgresIncrementalLoader


class Contacts(Caresoft):
    getter = IncrementalDetailsGetter
    loader = [
        BigQueryIncrementalLoader,
        PostgresIncrementalLoader,
    ]
    endpoint = row_key = "contacts"
    keys = {
        "p_key": ["id"],
        "incre_key": "updated_at",
    }
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "gender", "type": "INTEGER"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "email", "type": "STRING"},
        {"name": "phone_no", "type": "STRING"},
        {"name": "username", "type": "STRING"},
    ]

    columns = [
        Column("id", Integer, primary_key=True),
        Column("updated_at", DateTime(timezone=True)),
        Column("gender", Integer),
        Column("created_at", DateTime(timezone=True)),
        Column("email", String),
        Column("phone_no", String, index=True),
        Column("username", String),
    ]

    def transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "created_at": row.get("created_at"),
                "username": row.get("username"),
                "phone_no": row.get("phone_no"),
                "username": row.get("username"),
            }
            for row in rows
        ]
