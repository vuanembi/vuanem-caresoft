from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()


class Agents(Base):
    __tablename__ = "Agents"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String)
    email = Column(String)
    phone_no = Column(String)
    agent_id = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    group_id = Column(Integer)
    group_name = Column(String)
    role_id = Column(Integer)
    login_status = Column(String)
    call_status = Column(String)


class Groups(Base):
    __tablename__ = "Groups"

    group_id = Column(Integer, primary_key=True, index=True)
    group_name = Column(String)
    created_at = Column(DateTime(timezone=True))


class Services(Base):
    __tablename__ = "Services"

    service_id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String)
    service_type = Column(String)


class ContactsCustomFields(Base):
    __tablename__ = "ContactsCustomFields"

    custom_field_id = Column(Integer, primary_key=True, index=True)
    custom_field_lable = Column(String)
    type = Column(String)
    values = Column(JSONB)


class TicketsCustomFields(Base):
    __tablename__ = "TicketsCustomFields"

    custom_field_id = Column(Integer, primary_key=True, index=True)
    custom_field_lable = Column(String)
    type = Column(String)
    values = Column(JSONB)


class Calls(Base):
    __tablename__ = "Calls"

    id = Column(Integer, primary_key=True, index=True)
    start_time = Column(DateTime(timezone=True), index=True)
    customer_id = Column(Integer)
    call_id = Column(String)
    path = Column(String)
    path_download = Column(String)
    caller = Column(String)
    called = Column(String)
    user_id = Column(String)
    agent_id = Column(String)
    group_id = Column(Integer)
    call_type = Column(Integer)
    call_status = Column(String)
    end_time = Column(DateTime(timezone=True))
    wait_time = Column(String)
    hold_time = Column(String)
    talk_time = Column(String)
    end_status = Column(String)
    ticket_id = Column(Integer, index=True)
    last_agent_id = Column(String)
    last_user_id = Column(Integer)
    call_survey = Column(String)
    call_survey_result = Column(Integer)
    missed_reason = Column(String)


class Contacts(Base):
    __tablename__ = "Contacts"

    id = Column(Integer, primary_key=True, index=True)
    updated_at = Column(DateTime(timezone=True))
    gender = Column(Integer)
    created_at = Column(DateTime(timezone=True), index=True)
    email = Column(String)
    phone_no = Column(String, index=True)
    username = Column(String)


class Tickets(Base):
    __tablename__ = "Tickets"

    ticket_id = Column(Integer, primary_key=True, index=True)
    updated_at = Column(DateTime(timezone=True))
    ticket_no = Column(Integer)
    ticket_subject = Column(String)
    created_at = Column(DateTime(timezone=True), index=True)
    ticket_status = Column(String)
    ticket_source = Column(String)
    ticket_priority = Column(String)
    requester_id = Column(Integer)
    assignee_id = Column(Integer)
    assignee = Column(JSONB)
    requester = Column(JSONB)
    custom_fields = Column(JSONB)
    tags = Column(JSONB)
    ccs = Column(JSONB)
    follows = Column(JSONB)

class ContactsDetails(Base):
    __tablename__ = "ContactsDetails"

    id = Column(Integer, primary_key=True, index=True)
    updated_at = Column(DateTime(timezone=True))
    account_id = Column(Integer)
    username = Column(String)
    email = Column(String)
    email2 = Column(String)
    phone_no = Column(String, index=True)
    phone_no2 = Column(String)
    phone_no3 = Column(String)
    facebook = Column(String)
    gender = Column(Integer)
    organization_id = Column(Integer)
    created_at = Column(DateTime(timezone=True), index=True)
    role_id = Column(Integer)
    campaign_handler_id = Column(Integer)
    organization = Column(String)
    custom_fields = Column(JSONB)
    psid = Column(JSONB)

class DeletedContacts(Base):
    __tablename__ = "DeletedContacts"

    id = Column(Integer, primary_key=True, index=True)
    deleted = Column(Boolean, index=True)

class TicketsDetails(Base):
    __tablename__ = "TicketsDetails"

    ticket_id = Column(Integer, primary_key=True, index=True)
    account_id = Column(Integer)
    sla_id = Column(Integer)
    ticket_no = Column(Integer, index=True)
    requester_id = Column(Integer)
    group_id = Column(Integer)
    ticket_source_end_status = Column(Integer)
    assignee_id = Column(Integer)
    ticket_priority = Column(String)
    ticket_source = Column(String)
    ticket_status = Column(String)
    ticket_subject = Column(String)
    created_at = Column(DateTime(timezone=True), index=True)
    duedate = Column(DateTime(timezone=True), index=True)
    updated_at = Column(DateTime(timezone=True), index=True)
    satisfaction = Column(Integer)
    satisfaction_at = Column(String)
    satisfaction_send = Column(String)
    satisfaction_content = Column(String)
    campaign_id = Column(Integer)
    automessage_id = Column(Integer)
    feedback_status = Column(String)
    sla = Column(String)
    assignee = Column(JSONB)
    requester = Column(JSONB, index=True)
    campaign = Column(JSONB)
    comments = Column(JSONB)
    custom_fields = Column(JSONB)
    tags = Column(JSONB)
    ccs = Column(JSONB)
    follows = Column(JSONB)
    
class DeletedTickets(Base):
    __tablename__ = "DeletedTickets"

    ticket_id = Column(Integer, primary_key=True, index=True)
    deleted = Column(Boolean, index=True)
