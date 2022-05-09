from caresoft.pipeline.interface import Pipeline
from caresoft.repo import get_listing
from caresoft.request_parser import updated

pipeline = Pipeline(
    name="Contacts",
    params_fn=updated,
    get=get_listing("contacts", lambda x: x["contacts"]),
    transform=lambda rows: [
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
    schema=[
        {"name": "id", "type": "INTEGER"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "gender", "type": "INTEGER"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "email", "type": "STRING"},
        {"name": "phone_no", "type": "STRING"},
        {"name": "username", "type": "STRING"},
    ],
    id_key="id",
    cursor_key="updated_at",
)
