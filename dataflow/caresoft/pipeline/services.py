from caresoft.pipeline.interface import Pipeline
from caresoft.repo import get_dimension
from caresoft.request_parser import dimension

pipeline = Pipeline(
    name="Services",
    params_fn=dimension,
    get=get_dimension("services", lambda x: x.get("services")),
    transform=lambda row: {
        "service_id": row.get("service_id"),
        "service_name": row.get("service_name"),
        "service_type": row.get("service_type"),
    },
    schema=[
        {"name": "service_id", "type": "INTEGER"},
        {"name": "service_name", "type": "STRING"},
        {"name": "service_type", "type": "STRING"},
    ],
)
