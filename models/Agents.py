from models.base import simple_pipelines

Agents = simple_pipelines(
    {
        "name": "Agents",
        "endpoint": "agents",
        "row_key": "agents",
        "transform": lambda rows: [
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
        ],
        "schema": [
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
        ],
    }
)
