from caresoft.pipeline.interface import Pipeline
from caresoft.request_parser import dimension_request_parser

Services = Pipeline(
    name="Services",
    uri="services",
    res_fn=lambda x: x["services"],
    params_fn=dimension_request_parser,
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
