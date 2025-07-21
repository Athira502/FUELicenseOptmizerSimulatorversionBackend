from pydantic import BaseModel
class RoleDetailResponse(BaseModel):
    id: str
    profile: str
    description: str
    classification: str
    gb: int
    gc: int
    gd: int
    # not_classified: int
    assignedUsers: int


    class Config:
        orm_mode = True
