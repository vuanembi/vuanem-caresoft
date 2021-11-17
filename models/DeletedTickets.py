from sqlalchemy import Column, Integer

from models.models import Caresoft
from components.getter import DeletedGetter
from components.loader import BigQueryAppendLoader, PostgresIncrementalLoader


class DeletedTickets(Caresoft):
    getter = DeletedGetter
    endpoint = row_key = None
    loader = [
        PostgresIncrementalLoader,
        BigQueryAppendLoader,
    ]
    keys = {
        "p_key": ["ticket_id"],
    }

    schema = [
        {"name": "ticket_id", "type": "INTEGER"},
    ]

    columns = [
        Column("ticket_id", Integer),
    ]

    def __init__(self, rows):
        self.rows = rows
        super().__init__()

    def transform(self, rows):
        return [
            {
                "ticket_id": row["ticket_id"],
            }
            for row in rows
        ]
