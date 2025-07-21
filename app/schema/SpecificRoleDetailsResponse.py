from typing import List
from pydantic import BaseModel
from app.schema.RoleObjectDetail import RoleObjectDetail

class SpecificRoleDetailsResponse(BaseModel):
    roleName: str
    roleDescription: str
    objectDetails: List[RoleObjectDetail]