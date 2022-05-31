from caresoft.pipeline.interface import Pipeline
from caresoft.repo import get_dimension
from caresoft.request_parser import dimension

pipeline = Pipeline(
    name="ContactsCustomFields",
    params_fn=dimension,
    get=get_dimension("contacts/custom_fields", lambda x: x.get("custom_fields")),
    transform=lambda row: {
        "custom_field_id": row.get("custom_field_id"),
        "custom_field_lable": row.get("custom_field_lable"),
        "type": row.get("type"),
        "values": [
            {
                "id": value.get("id"),
                "lable": value.get("lable"),
            }
            for value in row.get("values", [])
        ],
    },
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
