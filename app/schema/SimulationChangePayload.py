
from typing import Optional, Literal

from pydantic import BaseModel

class SimulationChangePayload(BaseModel):
    role_id: str
    object: str
    field_name: str
    value_low: str
    value_high: str
    ttext: Optional[str] = None
    classification: Optional[str] = None
    action: Literal["Add", "Change", "Remove"] | None
    new_value_ui_text: Optional[str] = None
    is_new_object: bool
    frontend_id: int
