from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from models.models import Caresoft
from components.getter import SimpleGetter
from components.loader import BigQuerySimpleLoader, PostgresStandardLoader


class ContactsCustomFields(Caresoft):
    getter = SimpleGetter
    loader = [
        BigQuerySimpleLoader,
        PostgresStandardLoader,
    ]
    endpoint = f"contacts/custom_fields"
    row_key = "custom_fields"
    schema = [
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
    ]
    columns = [
        Column("custom_field_id", Integer, primary_key=True),
        Column("custom_field_lable", String),
        Column("type", String),
        Column("values", JSONB),
    ]

    def transform(self, rows):
        return [
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
        ]
