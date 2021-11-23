from models.base import simple_pipelines

TicketsCustomFields = simple_pipelines(
    {
        "name": "TicketsCustomFields",
        "endpoint": "tickets/custom_fields",
        "row_key": "custom_fields",
        "transform": lambda rows: [
            {
                "custom_field_id": row.get("custom_field_id"),
                "custom_field_lable": row.get("custom_field_lable"),
                "type": row.get("type"),
                "values": [
                    {
                        "id": value.get("id"),
                        "lable": value.get("lable"),
                    }
                    for value in row.get("values", [])
                ]
                if row.get("values")
                else [],
            }
            for row in rows
        ],
        "schema": [
            {"name": "custom_field_id", "type": "INTEGER"},
            {"name": "custom_field_lable", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {
                "name": "values",
                "type": "record",
                "mode": "repeated",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "lable", "type": "STRING"},
                ],
            },
        ],
    }
)
