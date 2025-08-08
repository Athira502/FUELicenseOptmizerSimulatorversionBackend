from typing import Optional
from pydantic import BaseModel

class LogLevelRequest(BaseModel):
    log_level: str

class LogLevelResponse(BaseModel):
    success: bool
    log_level: str
    client_name: Optional[str] = None
    system_id: Optional[str] = None

class LogLevelUpdateResponse(BaseModel):
    success: bool
    message: str
    old_level: Optional[str] = None
    new_level: Optional[str] = None
    timestamp: Optional[str] = None