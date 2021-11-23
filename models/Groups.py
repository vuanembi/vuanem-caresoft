from models.base import simple_pipelines

Groups = simple_pipelines(
    {
        "name": "Groups",
        "endpoint": "groups",
        "row_key": "groups",
        "transform": lambda rows: [
            {
                "group_id": row.get("group_id"),
                "group_name": row.get("group_name"),
                "created_at": row.get("created_at"),
            }
            for row in rows
        ],
        "schema": [
            {"name": "group_id", "type": "INTEGER"},
            {"name": "group_name", "type": "STRING"},
            {"name": "created_at", "type": "TIMESTAMP"},
        ],
    }
)
