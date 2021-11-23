from libs.caresoft import updated_params_builder
from models.base import incremental_pipelines

Tickets = incremental_pipelines(
    {
        "name": "Tickets",
        "endpoint": "tickets",
        "row_key": "tickets",
        "transform": lambda rows: [
            {
                "ticket_id": row.get("ticket_id"),
                "updated_at": row.get("updated_at"),
                "ticket_no": row.get("ticket_no"),
                "ticket_subject": row.get("ticket_subject"),
                "created_at": row.get("created_at"),
                "ticket_status": row.get("ticket_status"),
                "ticket_source": row.get("ticket_source"),
                "ticket_priority": row.get("ticket_priority"),
                "requester_id": row.get("requester_id"),
                "assignee_id": row.get("assignee_id"),
                "assignee": {
                    "id": row['assignee'].get("id"),
                    "username": row['assignee'].get("username"),
                    "email": row['assignee'].get("email"),
                    "phone_no": row['assignee'].get("phone_no"),
                    "agent_id": row['assignee'].get("agent_id"),
                    "role_id": row['assignee'].get("role_id"),
                    "group_id": row['assignee'].get("group_id"),
                    "group_name": row['assignee'].get("group_name"),
                }
                if row.get("assignee", {})
                else {},
                "requester": {
                    "id": row['requester'].get("id"),
                    "username": row['requester'].get("username"),
                    "email": row['requester'].get("email"),
                    "phone_no": row['requester'].get("phone_no"),
                    "organization_id": row['requester'].get("organization_id"),
                }
                if row.get("requester", {})
                else {},
                "custom_fields": [
                    {
                        "id": custom_field.get("id"),
                        "lable": custom_field.get("lable"),
                        "type": custom_field.get("type"),
                        "value": custom_field.get("value"),
                    }
                    for custom_field in row["custom_fields"]
                ]
                if row.get("custom_fields")
                else [],
                "tags": [
                    {
                        "name": tag.get("name"),
                        "tags": tag.get("tags"),
                    }
                    for tag in row["tags"]
                ]
                if row.get("tags")
                else [],
                "ccs": [
                    {
                        "id": cc.get("id"),
                        "username": cc.get("username"),
                        "email": cc.get("email"),
                    }
                    for cc in row["ccs"]
                ]
                if row.get("ccs")
                else [],
                "follows": [
                    {
                        "id": cc.get("id"),
                        "username": cc.get("username"),
                        "email": cc.get("email"),
                    }
                    for cc in row["follows"]
                ]
                if row.get("follows")
                else [],
            }
            for row in rows
        ],
        "schema": [
            {"name": "ticket_id", "type": "INTEGER"},
            {"name": "updated_at", "type": "TIMESTAMP"},
            {"name": "ticket_no", "type": "INTEGER"},
            {"name": "ticket_subject", "type": "STRING"},
            {"name": "created_at", "type": "TIMESTAMP"},
            {"name": "ticket_status", "type": "STRING"},
            {"name": "ticket_source", "type": "STRING"},
            {"name": "ticket_priority", "type": "STRING"},
            {"name": "requester_id", "type": "INTEGER"},
            {"name": "assignee_id", "type": "INTEGER"},
            {
                "name": "assignee",
                "type": "RECORD",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "username", "type": "STRING"},
                    {"name": "email", "type": "STRING"},
                    {"name": "phone_no", "type": "STRING"},
                    {"name": "agent_id", "type": "STRING"},
                    {"name": "role_id", "type": "INTEGER"},
                    {"name": "group_id", "type": "INTEGER"},
                    {"name": "group_name", "type": "STRING"},
                ],
            },
            {
                "name": "requester",
                "type": "RECORD",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "username", "type": "STRING"},
                    {"name": "email", "type": "STRING"},
                    {"name": "phone_no", "type": "STRING"},
                    {"name": "organization_id", "type": "INTEGER"},
                ],
            },
            {
                "name": "custom_fields",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "lable", "type": "STRING"},
                    {"name": "type", "type": "STRING"},
                    {"name": "value", "type": "STRING"},
                ],
            },
            {
                "name": "tags",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "tags", "type": "STRING"}
                ],
            },
            {
                "name": "ccs",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "username", "type": "STRING"},
                    {"name": "email", "type": "STRING"},
                ],
            },
            {
                "name": "follows",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "username", "type": "STRING"},
                    {"name": "email", "type": "STRING"},
                ],
            },
        ],
        "params_builder": updated_params_builder,
        "keys": {
            "p_key": "ticket_id",
            "incre_key": "updated_at",
        },
    }
)
