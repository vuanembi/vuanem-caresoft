from caresoft.pipeline.interface import Pipeline
from caresoft.request_parser import dimension_request_parser

pipeline = Pipeline(
    name="ContactsCustomFields",
    uri="contacts/custom_fields",
    res_fn=lambda x: x["custom_fields"],
    params_fn=dimension_request_parser,
    transform=lambda rows: [
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
    schema=[
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
)
