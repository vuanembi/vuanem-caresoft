import json

from components import (
    CaresoftStatic,
    CaresoftIncrementalStandard,
    CaresoftIncrementalDetails,
    CaresoftDetails,
)
import pg_models


class CaresoftFactory:
    @staticmethod
    def factory(table, start, end):
        if table == "Agents":
            return Agents()
        elif table == "Groups":
            return Groups()
        elif table == "Services":
            return Services()
        elif table == "ContactsCustomFields":
            return ContactsCustomFields()
        elif table == "TicketsCustomFields":
            return TicketsCustomFields()
        elif table == "Calls":
            return Calls(start, end)
        elif table == "Contacts":
            return Contacts(start, end)
        elif table == "Tickets":
            return Tickets(start, end)
        elif table == "ContactsDetails":
            return ContactsDetails()
        elif table == "TicketsDetails":
            return TicketsDetails()
        else:
            raise NotImplementedError(table)




class ContactsDetails(CaresoftDetails):
    table = "ContactsDetails"
    endpoint = parent = "contacts"
    row_key = "contact"
    detail_key = "id"
    model = pg_models.ContactsDetails
    deleted_model = pg_models.DeletedContacts

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "account_id": row.get("account_id"),
                "username": row.get("username"),
                "email": row.get("email"),
                "email2": row.get("email2"),
                "phone_no": row.get("phone_no"),
                "phone_no2": row.get("phone_no2"),
                "phone_no3": row.get("phone_no3"),
                "facebook": row.get("facebook"),
                "gender": row.get("gender"),
                "organization_id": row.get("organization_id"),
                "created_at": row.get("created_at"),
                "role_id": row.get("role_id"),
                "campaign_handler_id": row.get("campaign_handler_id"),
                "organization": json.dumps(row.get("organization")),
                "custom_fields": [
                    {
                        "id": custom_field.get("id"),
                        "lable": custom_field.get("lable"),
                        "type": custom_field.get("type"),
                        "value": custom_field.get("value"),
                    }
                    for custom_field in row.get("custom_fields", [])
                ]
                if row.get("custom_fields")
                else [],
                "psid": [
                    {
                        "page_id": psid.get("page_id"),
                        "psid": psid.get("psid"),
                    }
                    for psid in row.get("psid", [])
                ]
                if row.get("psid")
                else [],
            }
            for row in rows
        ]


class TicketsDetails(CaresoftDetails):
    table = "TicketsDetails"
    endpoint = parent = "tickets"
    row_key = "ticket"
    detail_key = "ticket_id"
    model = pg_models.TicketsDetails
    deleted_model = pg_models.DeletedTickets

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "ticket_id": row.get("ticket_id"),
                "updated_at": row.get("updated_at"),
                "account_id": row.get("account_id"),
                "sla_id": row.get("sla_id"),
                "ticket_no": row.get("ticket_no"),
                "requester_id": row.get("requester_id"),
                "group_id": row.get("group_id"),
                "ticket_source_end_status": row.get("ticket_source_end_status"),
                "assignee_id": row.get("assignee_id"),
                "ticket_priority": row.get("ticket_priority"),
                "ticket_source": row.get("ticket_source"),
                "ticket_status": row.get("ticket_status"),
                "ticket_subject": row.get("ticket_subject"),
                "created_at": row.get("created_at"),
                "duedate": row.get("duedate"),
                "satisfaction": row.get("satisfaction"),
                "satisfaction_at": row.get("satisfaction_at"),
                "satisfaction_send": row.get("satisfaction_send"),
                "satisfaction_content": row.get("satisfaction_content"),
                "campaign_id": row.get("campaign_id"),
                "automessage_id": row.get("automessage_id"),
                "feedback_status": row.get("feedback_status"),
                "sla": row.get("sla"),
                "assignee": {
                    "id": row.get("id"),
                    "username": row.get("username"),
                    "email": row.get("email"),
                    "phone_no": row.get("phone_no"),
                    "agent_id": row.get("agent_id"),
                    "role_id": row.get("role_id"),
                    "group_id": row.get("group_id"),
                    "group_name": row.get("group_name"),
                }
                if row.get("assignee")
                else {},
                "requester": {
                    "id": row.get("id"),
                    "username": row.get("username"),
                    "email": row.get("email"),
                    "phone_no": row.get("phone_no"),
                    "organization_id": row.get("organization_id"),
                }
                if row.get("requester")
                else {},
                "campaign": {
                    "id": row.get("id"),
                    "campaign_name": row.get("campaign_name"),
                    "status": row.get("status"),
                }
                if row.get("campaign")
                else {},
                "comments": [
                    {
                        "id": comment.get("id"),
                        "comment": comment.get("comment"),
                        "commentator_id": comment.get("commentator_id"),
                        "commentator_name": comment.get("commentator_name"),
                        "comment_source": comment.get("comment_source"),
                        "created_at": comment.get("created_at"),
                        "is_public": comment.get("is_public"),
                    }
                    for comment in row.get("comments", [])
                ]
                if row.get("comments")
                else [],
                "custom_fields": [
                    {
                        "id": custom_field.get("id"),
                        "lable": custom_field.get("lable"),
                        "type": custom_field.get("type"),
                        "value": custom_field.get("value"),
                    }
                    for custom_field in row.get("custom_fields", [])
                ]
                if row.get("custom_fields")
                else [],
                "tags": [
                    {
                        "name": tag.get("name"),
                        "tags": tag.get("tags"),
                    }
                    for tag in row.get("tags", [])
                ]
                if row.get("tags")
                else [],
                "ccs": [
                    {
                        "id": cc.get("id"),
                        "username": cc.get("username"),
                        "email": cc.get("email"),
                    }
                    for cc in row.get("ccs", [])
                ]
                if row.get("ccs")
                else [],
                "follows": [
                    {
                        "id": cc.get("id"),
                        "username": cc.get("username"),
                        "email": cc.get("email"),
                    }
                    for cc in row.get("follows", [])
                ]
                if row.get("follows")
                else [],
            }
            for row in rows
        ]
