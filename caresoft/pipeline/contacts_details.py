import json

from caresoft.pipeline.interface import Pipeline
from caresoft.request_parser import details_request_parser

pipeline = Pipeline(
    name="ContactsDetails",
    uri="contacts",
    res_fn=lambda x: x["contacts"],
    params_fn=details_request_parser,
    transform=lambda rows: [
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
                for custom_field in row["custom_fields"]
            ]
            if row.get("custom_fields")
            else [],
            "psid": [
                {
                    "page_id": psid.get("page_id"),
                    "psid": psid.get("psid"),
                }
                for psid in row["psid"]
            ]
            if row.get("psid", [])
            else [],
        }
        for row in rows
    ],
    schema=[
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
    ],
    id_key="id",
    cursor_key="updated_at",
)
