from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any

from app.core.logger import configure_logging
from app.models.database import get_db
from app.models.dynamic_models import _BaseLiceData
from app.models.request_array import RequestArray
from app.models.role_lic_re_results import LicenseOptimizationResult
from app.schema.LicenseOptimizationResult import LicenseOptimizationResultSchema
from app.schema.RequestArray import RequestArraySchema
from app.service.license_optimizer_service import optimize_license_logic, get_distinct_license_types_db

router = APIRouter(
    prefix="/optimize",
    tags=["License Optimization"]
)

@router.get("/license")
async def optimize_license_endpoint(
    client_name: str = Query(..., description="Client identifier (e.g., 'Fujifilm', 'ClientB')"),
    system_id:str = Query(..., description="System identifier (e.g., 'S4H', 'ClientB')"),
    ratio_threshold: Optional[int] = Query(None, description="Max AGR_RATIO value (first part) to include"),
    validation_type: str = Query("role", description="Validation mode ('role' or 'user')"),
    target_license: str = Query("GB Advanced Use", description="Target license type to analyze"),
    sap_system_info: str = Query("S4 HANA OnPremise 1909 Initial Support Pack", description="SAP system info for context"),
    role_names: Optional[List[str]] = Query(None, description="List of specific roles to analyze (optional)"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Analyzes SAP roles for a specific client to suggest FUE license optimization.
    Requires data to be loaded first via the /data/load-client-data endpoint.
    """
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name query parameter is required.")

    if not system_id:
        raise HTTPException(status_code=400, detail="system_id query parameter is required.")

    global logger
    logger = configure_logging(client_name=client_name, system_id=system_id)

    logger.info(
        f"Received request to optimize license for client: {client_name}, system_id: {system_id}, roles: {role_names or 'All'}")

    print(f"Received request to optimize license for client: {client_name},system_id:{system_id} roles: {role_names or 'All'}")

    result = await optimize_license_logic(
        db=db,
        client_name=client_name,
        system_id=system_id,
        ratio_threshold=ratio_threshold,
        validation_type=validation_type,
        target_license=target_license,
        sap_system_info=sap_system_info,
        role_names=role_names
    )

    if isinstance(result, dict) and "error" in result:
        status_code = result.get("status_code", 400)
        logger.error(f"Optimization failed with status {status_code}: {result['error']}")
        raise HTTPException(status_code=status_code, detail=result['error'])

    logger.info("License optimization completed successfully.")


    print(f"Optimization analysis completed for client: {client_name}")
    return result



@router.get("/requests", response_model=List[RequestArraySchema])
def get_all_requests(db: Session = Depends(get_db)):
    return db.query(RequestArray).order_by(RequestArray.TIMESTAMP.desc()).all()


@router.get("/results/{req_id}", response_model=List[LicenseOptimizationResultSchema])
def get_results_by_request_id(req_id: int, db: Session = Depends(get_db)):
    return db.query(LicenseOptimizationResult).filter(
        LicenseOptimizationResult.REQ_ID == req_id
    ).all()


@router.get("/license-types")
async def get_license_types_endpoint(
    client_name: str = Query(..., description="Client identifier (e.g., 'Fujifilm', 'ClientB')"),
    system_id: str = Query(..., description="System identifier (e.g., 'S4H', 'ClientB')"),
    db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    """
    Retrieves distinct license types for a given client and system.
    Requires data to be loaded first.
    """
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name query parameter is required.")
    if not system_id:
        raise HTTPException(status_code=400, detail="system_id query parameter is required.")
    return await get_distinct_license_types_db(db, client_name, system_id)
