import json

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB

from models.models import Caresoft
from models.DeletedContacts import DeletedContacts
from components.getter import DetailsGetter
from components.loader import BigQueryIncrementalLoader, PostgresIncrementalLoader


class ContactsDetails(Caresoft):
    getter = DetailsGetter
    loader = [
        BigQueryIncrementalLoader,
        PostgresIncrementalLoader,
    ]
    deleted_model = DeletedContacts
    endpoint = parent = "contacts"
    row_key = "contact"
    detail_key = "id"
    keys = {
        "p_key": ["id"],
        "incre_key": "updated_at",
    }

    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "account_id", "type": "INTEGER"},
        {"name": "username", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "email2", "type": "STRING"},
        {"name": "phone_no", "type": "STRING"},
        {"name": "phone_no2", "type": "STRING"},
        {"name": "phone_no3", "type": "STRING"},
        {"name": "facebook", "type": "STRING"},
        {"name": "gender", "type": "INTEGER"},
        {"name": "organization_id", "type": "INTEGER"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "role_id", "type": "INTEGER"},
        {"name": "campaign_handler_id", "type": "INTEGER"},
        {"name": "organization", "type": "STRING"},
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
            "name": "psid",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "page_id", "type": "STRING"},
                {"name": "psid", "type": "INTEGER"},
            ],
        },
    ]

    columns = [
        Column("id", Integer, primary_key=True),
        Column("updated_at", DateTime(timezone=True)),
        Column("account_id", Integer),
        Column("username", String),
        Column("email", String),
        Column("email2", String),
        Column("phone_no", String),
        Column("phone_no2", String),
        Column("phone_no3", String),
        Column("facebook", String),
        Column("gender", Integer),
        Column("organization_id", Integer),
        Column("created_at", DateTime(timezone=True)),
        Column("role_id", Integer),
        Column("campaign_handler_id", Integer),
        Column("organization", String),
        Column("custom_fields", JSONB),
        Column("psid", JSONB),
    ]

    def transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "account_id": row.get("account_id"),
                "username": row.get("username"),
                "email": row.get("email"),
                "email2": row.get("email2"),
                "phone_no": row.get("phone_no"),
                "phone_no2": row.get("phone_no2"),
                "phone_no3": row.get("phone_no3"),
                "facebook": row.get("facebook"),
                "gender": row.get("gender"),
                "organization_id": row.get("organization_id"),
                "created_at": row.get("created_at"),
                "role_id": row.get("role_id"),
                "campaign_handler_id": row.get("campaign_handler_id"),
                "organization": json.dumps(row.get("organization")),
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
                "psid": [
                    {
                        "page_id": psid.get("page_id"),
                        "psid": psid.get("psid"),
                    }
                    for psid in row.get("psid", [])
                ]
                if row.get("psid")
                else [],
            }
            for row in rows
        ]
