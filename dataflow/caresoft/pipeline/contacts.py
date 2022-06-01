from caresoft.pipeline import interface, contacts_details
from caresoft.repo import get_listing
from caresoft.request_parser import updated
from caresoft.pipeline import contacts_details

pipeline = interface.Pipeline(
    name="Contacts",
    params_fn=updated,
    get=get_listing("contacts", lambda x: x.get("contacts")),
    transform=lambda row: {
        "id": row.get("id"),
        "updated_at": row.get("updated_at"),
        "created_at": row.get("created_at"),
        "username": row.get("username"),
        "phone_no": row.get("phone_no"),
        "username": row.get("username"),
        "_cursor": row.get("updated_at"),
    },
    schema=[
        {"name": "id", "type": "INTEGER"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "gender", "type": "INTEGER"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "email", "type": "STRING"},
        {"name": "phone_no", "type": "STRING"},
        {"name": "username", "type": "STRING"},
        {"name": "_cursor", "type": "TIMESTAMP"},
    ],
    key=interface.Key("id", "created_at"),
    details=contacts_details.pipeline,
)
