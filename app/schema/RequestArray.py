
from pydantic import BaseModel
from datetime import datetime

class RequestArraySchema(BaseModel):
    req_id: str
    TIMESTAMP: datetime
    CLIENT_NAME: str
    SYSTEM_NAME: str
    STATUS: str

    class Config:
        orm_mode = True
