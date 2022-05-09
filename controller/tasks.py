from libs.tasks import create_tasks

TABLES = {
    "dimensions": [
        "Agents",
        "Groups",
        "Services",
        "TicketsCustomFields",
        "ContactsCustomFields",
    ],
    "incremental": [
        "Tickets",
        "Contacts",
        "Calls",
    ],
    "details": [
        "TicketsDetails",
        "ContactsDetails",
    ],
}


def orchestrate(tasks: str) -> dict:
    return {
        "tasks": tasks,
        "tasks_created": create_tasks(
            [
                {
                    "table": table,
                }
                for table in TABLES[tasks]
            ]
        ),
    }
