from sqlalchemy import Column, String, Integer, ForeignKey
from app.models.database import Base


class LicenseOptimizationResult(Base):
    __tablename__ = "Z_FUE_OPT_RESULTS"

    RESULT_ID = Column(Integer, primary_key=True, autoincrement=True)
    REQ_ID= Column(Integer, ForeignKey("Z_FUE_OPT_REQUESTS.req_id"), nullable=False)
    ROLE_ID= Column(String,nullable=False)
    ROLE_DESCRIPTION= Column(String,nullable=False)
    AUTHORIZATION_OBJECT=Column(String,nullable=False)
    FIELD=Column(String,nullable=False)
    VALUE=Column(String,nullable=False)
    LICENSE_REDUCIBLE=Column(String,nullable=False)
    INSIGHTS=Column(String,nullable=False)
    RECOMMENDATIONS=Column(String)
    EXPLANATIONS=Column(String)
