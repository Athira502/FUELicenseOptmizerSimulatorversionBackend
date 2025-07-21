from typing import Optional

from pydantic import BaseModel

class RoleObjectDetail(BaseModel):
    object: str
    classification: str
    fieldName: str
    valueLow: str
    valueHigh: Optional[str] = None  # Make valueHigh optional
    ttext: Optional[str] = None  # Make ttext optional
