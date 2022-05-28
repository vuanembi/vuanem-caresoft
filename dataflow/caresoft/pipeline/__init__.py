from caresoft.pipeline import (
    agents,
    calls,
    contacts_custom_fields,
    contacts_details,
    contacts,
    groups,
    services,
    tickets,
    tickets_custom_fields,
    tickets_details,
)

dimension = [
    agents,
    contacts_custom_fields,
    groups,
    services,
    tickets_custom_fields,
]
listing = [calls, contacts, tickets]
details = [contacts_details, tickets_details]

dimension_pipelines, listing_pipelines, details_pipelines = [
    {i.name: i for i in [j.pipeline for j in l]}  # type: ignore
    for l in [
        dimension,
        listing,
        details,
    ]
]

pipelines = dimension_pipelines | listing_pipelines
