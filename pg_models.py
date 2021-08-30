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
