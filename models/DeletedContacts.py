from sqlalchemy import Column, Integer

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
    ]

    columns = [
        Column("id", Integer),
    ]

    def __init__(self, rows):
        self.rows = rows
        super().__init__()

    def transform(self, rows):
        return [
            {
                "id": row["id"],
            }
            for row in rows
        ]
