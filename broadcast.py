import os
import json

from google.cloud import pubsub_v1


PUBLISHER = pubsub_v1.PublisherClient()
TOPIC_PATH = PUBLISHER.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))
TABLES = [
    "Services",
    "TicketsDetails",
    "Agents",
    "ContactsDetails",
    "ContactsCustomFields",
    "Tickets",
    "Groups",
    "TicketsCustomFields",
    "Calls",
    "Contacts",
]


def broadcast():
    for table in TABLES:
        message_json = json.dumps(
            {
                "table": table,
            }
        )
        message_bytes = message_json.encode("utf-8")
        PUBLISHER.publish(TOPIC_PATH, data=message_bytes).result()
    return {
        "message_sent": len(TABLES),
    }
