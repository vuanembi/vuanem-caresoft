from sqlalchemy import Column, Integer, String, DateTime

from models.models import CaresoftIncrementalStandard


class Calls(CaresoftIncrementalStandard):
    endpoint = row_key = "calls"
    keys = {"p_key": ["id"], "incre_key": "start_time"}
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "start_time", "type": "TIMESTAMP"},
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "call_id", "type": "STRING"},
        {"name": "path", "type": "STRING"},
        {"name": "path_download", "type": "STRING"},
        {"name": "caller", "type": "STRING"},
        {"name": "called", "type": "STRING"},
        {"name": "user_id", "type": "STRING"},
        {"name": "agent_id", "type": "STRING"},
        {"name": "group_id", "type": "INTEGER"},
        {"name": "call_type", "type": "INTEGER"},
        {"name": "call_status", "type": "STRING"},
        {"name": "end_time", "type": "TIMESTAMP"},
        {"name": "wait_time", "type": "STRING"},
        {"name": "hold_time", "type": "STRING"},
        {"name": "talk_time", "type": "STRING"},
        {"name": "end_status", "type": "STRING"},
        {"name": "ticket_id", "type": "INTEGER"},
        {"name": "last_agent_id", "type": "STRING"},
        {"name": "last_user_id", "type": "INTEGER"},
        {"name": "call_survey", "type": "STRING"},
        {"name": "call_survey_result", "type": "INTEGER"},
        {"name": "missed_reason", "type": "STRING"},
    ]

    columns = [
        Column("id", Integer, primary_key=True),
        Column("start_time", DateTime(timezone=True)),
        Column("customer_id", Integer),
        Column("call_id", String),
        Column("path", String),
        Column("path_download", String),
        Column("caller", String),
        Column("called", String),
        Column("user_id", String),
        Column("agent_id", String),
        Column("group_id", Integer),
        Column("call_type", Integer),
        Column("call_status", String),
        Column("end_time", DateTime(timezone=True)),
        Column("wait_time", String),
        Column("hold_time", String),
        Column("talk_time", String),
        Column("end_status", String),
        Column("ticket_id", Integer, index=True),
        Column("last_agent_id", String),
        Column("last_user_id", Integer),
        Column("call_survey", String),
        Column("call_survey_result", Integer),
        Column("missed_reason", String),
    ]

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "customer_id": row.get("customer_id"),
                "call_id": row.get("call_id"),
                "path": row.get("path"),
                "path_download": row.get("path_download"),
                "caller": row.get("caller"),
                "called": row.get("called"),
                "user_id": row.get("user_id"),
                "agent_id": row.get("agent_id"),
                "group_id": row.get("group_id"),
                "call_type": row.get("call_type"),
                "start_time": row.get("start_time"),
                "call_status": row.get("call_status"),
                "end_time": row.get("end_time"),
                "wait_time": row.get("wait_time"),
                "hold_time": row.get("hold_time"),
                "talk_time": row.get("talk_time"),
                "end_status": row.get("end_status"),
                "ticket_id": row.get("ticket_id"),
                "last_agent_id": row.get("last_agent_id"),
                "last_user_id": row.get("last_user_id"),
                "call_survey": row.get("call_survey"),
                "call_survey_result": row.get("call_survey_result"),
                "missed_reason": row.get("missed_reason"),
            }
            for row in rows
        ]
