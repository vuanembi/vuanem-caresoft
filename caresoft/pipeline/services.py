from caresoft.pipeline.interface import Pipeline
from caresoft.repo import get_dimension
from caresoft.request_parser import dimension

Services = Pipeline(
    name="Services",
    params_fn=dimension,
    get=get_dimension("services",lambda x: x["services"]),
    transform=lambda rows: [
        {
            "service_id": row.get("service_id"),
            "service_name": row.get("service_name"),
            "service_type": row.get("service_type"),
        }
        for row in rows
    ],
    schema=[
        {"name": "service_id", "type": "INTEGER"},
        {"name": "service_name", "type": "STRING"},
        {"name": "service_type", "type": "STRING"},
    ],
)
