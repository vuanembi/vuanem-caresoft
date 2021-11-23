from libs.caresoft import time_params_builder
from models.base import incremental_pipelines

Contacts = incremental_pipelines(
    {
        "name": "Contacts",
        "endpoint": "contacts",
        "row_key": "contacts",
        "transform": lambda rows: [
            {
                "id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "created_at": row.get("created_at"),
                "username": row.get("username"),
                "phone_no": row.get("phone_no"),
                "username": row.get("username"),
            }
            for row in rows
        ],
        "schema": [
            {"name": "id", "type": "INTEGER"},
            {"name": "updated_at", "type": "TIMESTAMP"},
            {"name": "gender", "type": "INTEGER"},
            {"name": "created_at", "type": "TIMESTAMP"},
            {"name": "email", "type": "STRING"},
            {"name": "phone_no", "type": "STRING"},
            {"name": "username", "type": "STRING"},
        ],
        "params_builder": time_params_builder,
        "keys": {
            "p_key": "id",
            "incre_key": "updated_at",
        },
    }
)
