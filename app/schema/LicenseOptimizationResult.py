from typing import Optional

from pydantic import BaseModel


class LicenseOptimizationResultSchema(BaseModel):
    RESULT_ID: int
    REQ_ID: int
    ROLE_ID: str
    ROLE_DESCRIPTION: str
    AUTHORIZATION_OBJECT: str
    FIELD: str
    VALUE: str
    LICENSE_REDUCIBLE: str
    INSIGHTS: str
    RECOMMENDATIONS: Optional[str]
    EXPLANATIONS: Optional[str]

    class Config:
        orm_mode = True
