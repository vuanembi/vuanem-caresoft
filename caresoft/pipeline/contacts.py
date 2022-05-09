from caresoft.pipeline.interface import Pipeline
from caresoft.request_parser import updated_request_parser

pipeline = Pipeline(
    name="Contacts",
    uri="contacts",
    res_fn="contacts",
    params_fn=updated_request_parser,
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
