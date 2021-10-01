from sqlalchemy import Column, Integer, Boolean

from models.models import Caresoft
from components.getter import DeletedGetter
from components.loader import BigQueryAppendLoader, PostgresIncrementalLoader


class DeletedContacts(Caresoft):
    getter = DeletedGetter
    endpoint = row_key = None
    loader = [
        BigQueryAppendLoader,
        PostgresIncrementalLoader,
    ]
    keys = {
        "p_key": ["id"],
    }
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "deleted", "type": "BOOLEAN"},
    ]

    columns = [
        Column("id", Integer, primary_key=True),
        Column("deleted", Boolean),
    ]

    def __init__(self, rows):
        self.rows = rows
        super().__init__()

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "deleted": row.get("deleted"),
            }
            for row in rows
        ]
