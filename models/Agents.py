from sqlalchemy import Column, Integer, String, DateTime

from models.models import CaresoftStatic
from components.getter import SimpleGetter
from components.loader import BigQuerySimpleLoader, PostgresStandardLoader

class Agents(CaresoftStatic):
    getter = SimpleGetter
    loader = [
        BigQuerySimpleLoader,
        PostgresStandardLoader,
    ]
    endpoint = row_key = "agents"
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "username", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "phone_no", "type": "STRING"},
        {"name": "agent_id", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "group_id", "type": "INTEGER"},
        {"name": "group_name", "type": "STRING"},
        {"name": "role_id", "type": "INTEGER"},
        {"name": "login_status", "type": "STRING"},
        {"name": "call_status", "type": "STRING"},
    ]
    columns = [
        Column("id", Integer, primary_key=True),
        Column("username", String),
        Column("email", String),
        Column("phone_no", String),
        Column("agent_id", String),
        Column("created_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
        Column("group_id", Integer),
        Column("group_name", String),
        Column("role_id", Integer),
        Column("login_status", String),
        Column("call_status", String),
    ]

    def transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "username": row.get("username"),
                "email": row.get("email"),
                "phone_no": row.get("phone_no"),
                "agent_id": row.get("agent_id"),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "group_id": row.get("group_id"),
                "group_name": row.get("group_name"),
                "role_id": row.get("role_id"),
                "login_status": row.get("login_status"),
                "call_status": row.get("call_status"),
            }
            for row in rows
        ]
