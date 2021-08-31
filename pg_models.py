from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()


class Agents(Base):
    __tablename__ = "Agents"

    id = Column(Integer, primary_key=True)
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

    group_id = Column(Integer, primary_key=True)
    group_name = Column(String)
    created_at = Column(DateTime(timezone=True))


class Services(Base):
    __tablename__ = "Services"

    service_id = Column(Integer, primary_key=True)
    service_name = Column(String)
    service_type = Column(String)


class ContactsCustomFields(Base):
    __tablename__ = "ContactsCustomFields"

    custom_field_id = Column(Integer, primary_key=True)
    custom_field_lable = Column(String)
    type = Column(String)
    values = Column(JSONB)


class TicketsCustomFields(Base):
    __tablename__ = "TicketsCustomFields"

    custom_field_id = Column(Integer, primary_key=True)
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

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime(timezone=True))
    gender = Column(Integer)
    created_at = Column(DateTime(timezone=True))
    email = Column(String)
    phone_no = Column(String)
    username = Column(String)


class Tickets(Base):
    __tablename__ = "Tickets"

    ticket_id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime(timezone=True))
    ticket_no = Column(Integer)
    ticket_subject = Column(String)
    created_at = Column(DateTime(timezone=True))
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
