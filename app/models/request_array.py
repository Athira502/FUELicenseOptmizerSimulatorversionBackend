import uuid

from sqlalchemy import Column, String, Integer, DateTime, func
from app.models.database import Base

class RequestArray(Base):
    __tablename__ = "Z_FUE_OPT_REQUESTS"

    # req_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    req_id = Column(String, primary_key=True, index=True, default=lambda: f"REQ-{uuid.uuid4()}")
    TIMESTAMP = Column(DateTime, default=func.now())
    CLIENT_NAME = Column(String, nullable=False)
    SYSTEM_NAME = Column(String, nullable=False)
    STATUS = Column(String, nullable=False)
