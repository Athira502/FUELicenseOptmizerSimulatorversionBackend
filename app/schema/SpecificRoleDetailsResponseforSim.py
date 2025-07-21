from typing import List
from pydantic import BaseModel
from app.schema.RoleObjectDetail import RoleObjectDetail

class SpecificRoleDetailsResponseforSim(BaseModel):
    roleName: str
    objectDetails: List[RoleObjectDetail]