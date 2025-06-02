from sqlalchemy import Column, String, Integer
from app.models.database import Base

class clientSysReleaseData(Base):
    __tablename__ = "Z_FUE_CLIENT_SYS_INFO"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    CLIENT_NAME=Column(String, nullable=False)
    SYSTEM_NAME = Column(String, nullable=False)
    SYSTEM_RELEASE_INFO =Column(String, nullable=False)

