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


class Agents(CaresoftStatic):
    table = "Agents"
    endpoint = row_key = "agents"
    model = pg_models.Agents

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "username": row["username"],
                "email": row["email"],
                "phone_no": row["phone_no"],
                "agent_id": row["agent_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "group_id": row["group_id"],
                "group_name": row["group_name"],
                "role_id": row["role_id"],
                "login_status": row["login_status"],
                "call_status": row["call_status"],
            }
            for row in rows
        ]


class Groups(CaresoftStatic):
    table = "Groups"
    endpoint = row_key = "groups"
    model = pg_models.Groups

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "group_id": row["group_id"],
                "group_name": row["group_name"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]


class Services(CaresoftStatic):
    table = "Services"
    endpoint = row_key = "services"
    model = pg_models.Services

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "service_id": row["service_id"],
                "service_name": row["service_name"],
                "service_type": row["service_type"],
            }
            for row in rows
        ]


class ContactsCustomFields(CaresoftStatic):
    table = "ContactsCustomFields"
    endpoint = f"contacts/custom_fields"
    row_key = "custom_fields"
    model = pg_models.ContactsCustomFields

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "custom_field_id": row["custom_field_id"],
                "custom_field_lable": row["custom_field_lable"],
                "type": row.get("type"),
                "values": [
                    {
                        "id": value.get("id"),
                        "lable": value.get("lable"),
                    }
                    for value in row.get("values", [])
                ]
                if row.get("values")
                else [],
            }
            for row in rows
        ]


class TicketsCustomFields(CaresoftStatic):
    table = "TicketsCustomFields"
    endpoint = f"tickets/custom_fields"
    row_key = "custom_fields"
    model = pg_models.TicketsCustomFields

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "custom_field_id": row["custom_field_id"],
                "custom_field_lable": row["custom_field_lable"],
                "type": row.get("type"),
                "values": [
                    {
                        "id": value["id"],
                        "lable": value["lable"],
                    }
                    for value in row.get("values", [])
                ]
                if row.get("values")
                else [],
            }
            for row in rows
        ]


class Calls(CaresoftIncrementalStandard):
    table = "Calls"
    endpoint = row_key = "calls"

    def __init__(self, start, end):
        super().__init__(start, end)

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "customer_id": row["customer_id"],
                "call_id": row["call_id"],
                "caller": row["caller"],
                "called": row["called"],
                "user_id": row["user_id"],
                "agent_id": row["agent_id"],
                "group_id": row["group_id"],
                "call_type": row["call_type"],
                "start_time": row["start_time"],
                "call_status": row["call_status"],
                "end_time": row["end_time"],
                "wait_time": row["wait_time"],
                "hold_time": row["hold_time"],
                "talk_time": row["talk_time"],
                "end_status": row["end_status"],
                "ticket_id": row["ticket_id"],
                "last_agent_id": row["last_agent_id"],
                "last_user_id": row["last_user_id"],
                "call_survey": row["call_survey"],
                "call_survey_result": row["call_survey_result"],
                "missed_reason": row["missed_reason"],
            }
            for row in rows
        ]


class Contacts(CaresoftIncrementalDetails):
    table = "Contacts"
    endpoint = row_key = "contacts"

    def __init__(self, start, end):
        super().__init__(start, end)

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "updated_at": row["updated_at"],
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

    def __init__(self, start, end):
        super().__init__(start, end)

    def transform(self, rows):
        return [
            {
                "ticket_id": row["ticket_id"],
                "updated_at": row["updated_at"],
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

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "id": row["id"],
                "updated_at": row["updated_at"],
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

    def __init__(self):
        super().__init__()

    def transform(self, rows):
        return [
            {
                "ticket_id": row["ticket_id"],
                "updated_at": row["updated_at"],
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
