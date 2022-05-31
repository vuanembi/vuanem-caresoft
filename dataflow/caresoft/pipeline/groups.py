from caresoft.pipeline.interface import Pipeline
from caresoft.repo import get_dimension
from caresoft.request_parser import dimension

pipeline = Pipeline(
    name="Groups",
    params_fn=dimension,
    get=get_dimension("groups", lambda x: x.get("groups")),
    transform=lambda row: {
        "group_id": row.get("group_id"),
        "group_name": row.get("group_name"),
        "created_at": row.get("created_at"),
    },
    schema=[
        {"name": "group_id", "type": "INTEGER"},
        {"name": "group_name", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
    ],
)
