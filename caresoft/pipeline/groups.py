from caresoft.pipeline.interface import Pipeline
from caresoft.request_parser import dimension_request_parser

pipeline = Pipeline(
    name="Groups",
    uri="groups",
    res_fn=lambda x: x["groups"],
    params_fn=dimension_request_parser,
    transform=lambda rows: [
        {
            "group_id": row.get("group_id"),
            "group_name": row.get("group_name"),
            "created_at": row.get("created_at"),
        }
        for row in rows
    ],
    schema=[
        {"name": "group_id", "type": "INTEGER"},
        {"name": "group_name", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
)
