from models.base import simple_pipelines

Services = simple_pipelines(
    {
        "name": "Services",
        "endpoint": "services",
        "row_key": "services",
        "transform": lambda rows: [
            {
                "service_id": row.get("service_id"),
                "service_name": row.get("service_name"),
                "service_type": row.get("service_type"),
            }
            for row in rows
        ],
        "schema": [
            {"name": "service_id", "type": "INTEGER"},
            {"name": "service_name", "type": "STRING"},
            {"name": "service_type", "type": "STRING"},
        ],
    }
)
