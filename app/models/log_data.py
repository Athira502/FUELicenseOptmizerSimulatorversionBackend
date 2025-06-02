from sqlalchemy import Column, String, Integer, DateTime, func
from app.models.database import Base

class logData(Base):
    __tablename__ = "Z_FUE_LOG_FILE"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    TIMESTAMP= Column(DateTime, default=func.now())
    FILENAME=Column(String,nullable=False)
    CLIENT_NAME=Column(String, nullable=False)
    SYSTEM_NAME = Column(String, nullable=False)
    SYSTEM_RELEASE_INFO =Column(String, nullable=False)
    STATUS=Column(String, nullable=False)
    LOG_DATA=Column(String)

