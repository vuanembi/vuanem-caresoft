from models.base import details_pipelines

TicketsDetails = details_pipelines(
    {
        "name": "TicketsDetails",
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
                    "id": row.get("id"),
                    "username": row.get("username"),
                    "email": row.get("email"),
                    "phone_no": row.get("phone_no"),
                    "agent_id": row.get("agent_id"),
                    "role_id": row.get("role_id"),
                    "group_id": row.get("group_id"),
                    "group_name": row.get("group_name"),
                }
                if row.get("assignee")
                else {},
                "requester": {
                    "id": row.get("id"),
                    "username": row.get("username"),
                    "email": row.get("email"),
                    "phone_no": row.get("phone_no"),
                    "organization_id": row.get("organization_id"),
                }
                if row.get("requester")
                else {},
                "custom_fields": [
                    {
                        "id": custom_field.get("id"),
                        "lable": custom_field.get("lable"),
                        "type": custom_field.get("type"),
                        "value": custom_field.get("value"),
                    }
                    for custom_field in row.get("custom_fields", [])
                ]
                if row.get("custom_fields")
                else [],
                "tags": [
                    {
                        "name": tag.get("name"),
                        "tags": tag.get("tags"),
                    }
                    for tag in row.get("tags", [])
                ]
                if row.get("tags")
                else [],
                "ccs": [
                    {
                        "id": cc.get("id"),
                        "username": cc.get("username"),
                        "email": cc.get("email"),
                    }
                    for cc in row.get("ccs", [])
                ]
                if row.get("ccs")
                else [],
                "follows": [
                    {
                        "id": cc.get("id"),
                        "username": cc.get("username"),
                        "email": cc.get("email"),
                    }
                    for cc in row.get("follows", [])
                ]
                if row.get("follows")
                else [],
            }
            for row in rows
        ],
        "schema": [
            {"name": "ticket_id", "type": "INTEGER"},
            {"name": "account_id", "type": "INTEGER"},
            {"name": "sla_id", "type": "INTEGER"},
            {"name": "ticket_no", "type": "INTEGER"},
            {"name": "requester_id", "type": "INTEGER"},
            {"name": "group_id", "type": "INTEGER"},
            {"name": "ticket_source_end_status", "type": "INTEGER"},
            {"name": "assignee_id", "type": "INTEGER"},
            {"name": "ticket_priority", "type": "STRING"},
            {"name": "ticket_source", "type": "STRING"},
            {"name": "ticket_status", "type": "STRING"},
            {"name": "ticket_subject", "type": "STRING"},
            {"name": "created_at", "type": "TIMESTAMP"},
            {"name": "duedate", "type": "TIMESTAMP"},
            {"name": "updated_at", "type": "TIMESTAMP"},
            {"name": "satisfaction", "type": "INTEGER"},
            {"name": "satisfaction_at", "type": "STRING"},
            {"name": "satisfaction_send", "type": "STRING"},
            {"name": "satisfaction_content", "type": "STRING"},
            {"name": "campaign_id", "type": "INTEGER"},
            {"name": "automessage_id", "type": "INTEGER"},
            {"name": "feedback_status", "type": "STRING"},
            {"name": "sla", "type": "STRING"},
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
                "name": "campaign",
                "type": "RECORD",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "campaign_name", "type": "STRING"},
                    {"name": "status", "type": "INTEGER"},
                ],
            },
            {
                "name": "comments",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "comment", "type": "STRING"},
                    {"name": "commentator_id", "type": "INTEGER"},
                    {"name": "commentator_name", "type": "STRING"},
                    {"name": "comment_source", "type": "STRING"},
                    {"name": "created_at", "type": "TIMESTAMP"},
                    {"name": "is_public", "type": "INTEGER"},
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
        "keys": {
            "p_key": "ticket_id",
            "incre_key": "updated_at",
        },
    }
)
