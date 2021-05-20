from sqlalchemy import create_engine
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy import Integer,String
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

# eng = create_engine(os.environ.get("SQLALCHEMY_URL"))
Base = declarative_base()

class Event(Base):
    __tablename__ = "event"

    Id = Column(Integer, name="id", primary_key=True, autoincrement=True, nullable=False)
    EventKey = Column(String, name="event_key", unique=True)
    Name = Column(String, name="name")
    Data = Column(postgresql.JSONB, name="data")

class WampData(Base):
    __tablename__ = "wampdata"
    id = Column(Integer, name="id", primary_key=True)        
    EventId = Column(Integer, ForeignKey("event.id"),  name="event_id", nullable=False)
    Data = Column(postgresql.JSONB, name="data")
    
    Event = relationship("Event")

