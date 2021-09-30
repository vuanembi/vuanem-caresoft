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


class Calls(CaresoftIncrementalStandard):
    table = "Calls"
    endpoint = row_key = "calls"
    model = pg_models.Calls

    def __init__(self, start, end):
        super().__init__(start, end)

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "customer_id": row.get("customer_id"),
                "call_id": row.get("call_id"),
                "path": row.get("path"),
                "path_download": row.get("path_download"),
                "caller": row.get("caller"),
                "called": row.get("called"),
                "user_id": row.get("user_id"),
                "agent_id": row.get("agent_id"),
                "group_id": row.get("group_id"),
                "call_type": row.get("call_type"),
                "start_time": row.get("start_time"),
                "call_status": row.get("call_status"),
                "end_time": row.get("end_time"),
                "wait_time": row.get("wait_time"),
                "hold_time": row.get("hold_time"),
                "talk_time": row.get("talk_time"),
                "end_status": row.get("end_status"),
                "ticket_id": row.get("ticket_id"),
                "last_agent_id": row.get("last_agent_id"),
                "last_user_id": row.get("last_user_id"),
                "call_survey": row.get("call_survey"),
                "call_survey_result": row.get("call_survey_result"),
                "missed_reason": row.get("missed_reason"),
            }
            for row in rows
        ]


class Contacts(CaresoftIncrementalDetails):
    table = "Contacts"
    endpoint = row_key = "contacts"
    model = pg_models.Contacts

    def __init__(self, start, end):
        super().__init__(start, end)

    def transform(self, rows):
        return [
            {
                "id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "created_at": row.get("created_at"),
                "username": row.get("username"),
                "phone_no": row.get("phone_no"),
                "username": row.get("username"),
            }
            for row in rows
        ]


class Tickets(CaresoftIncrementalDetails):
    table = "Tickets"
    endpoint = row_key = "tickets"
    model = pg_models.Tickets

    def __init__(self, start, end):
        super().__init__(start, end)

    def transform(self, rows):
        return [
            {
                "ticket_id": row.get("ticket_id"),
                "updated_at": row.get("updated_at"),
                "ticket_no": row.get("ticket_no"),
                "ticket_subject": row.get("ticket_subject"),
                "created_at": row.get("created_at"),
                "ticket_status": row.get("ticket_status"),
                "ticket_source": row.get("ticket_source"),
                "ticket_priority": row.get("ticket_priority"),
                "requester_id": row.get("requester_id"),
                "assignee_id": row.get("assignee_id"),
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
