from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any

from app.core.logger import setup_logger, get_daily_log_filename
from app.models.database import get_db
from app.models.role_lic_re_results import LicenseOptimizationResult
from app.schema.LicenseOptimizationResult import LicenseOptimizationResultSchema
from app.schema.RequestArray import RequestArraySchema
from app.service.license_optimizer_service import  get_distinct_license_types_db, \
    get_all_requests_service, create_optimization_request_immediately, process_optimization_in_background

router = APIRouter(
    prefix="/optimize",
    tags=["License Optimization"]
)
logger = setup_logger("app_logger")

@router.get("/license")
async def optimize_license_endpoint(
        background_tasks: BackgroundTasks,
        client_name: str = Query(..., description="Client identifier (e.g., 'Fujifilm', 'ClientB')"),
        system_id: str = Query(..., description="System identifier (e.g., 'S4H', 'ClientB')"),
        ratio_threshold: Optional[int] = Query(None, description="Max AGR_RATIO value (first part) to include"),
        target_license: str = Query("GB Advanced Use", description="Target license type to analyze"),
        sap_system_info: str = Query("S4 HANA OnPremise 1909 Initial Support Pack",
                                     description="SAP system info for context"),
        role_names: Optional[List[str]] = Query(None, description="List of specific roles to analyze (optional)"),
        db: Session = Depends(get_db)
) -> Dict[str, Any]:

    """
    Initiates SAP role optimization analysis and returns immediately with request ID.
    The actual processing happens in the background.
    """
    logger.info(
        f"Received license optimization request for client: '{client_name}', system_id: '{system_id}', "
        f"roles: '{role_names or 'All'}', target_license: '{target_license}'"
    )
    if not client_name:
        logger.error(f"client name not present in the license optimization request for the roles:{role_names}")
        raise HTTPException(status_code=400, detail="client_name query parameter is required.")

    if not system_id:
        logger.error(f"system name not present in the license optimization request for the roles:{role_names}")
        raise HTTPException(status_code=400, detail="system_id query parameter is required.")




    try:
        logger.debug(f"Creating optimization request record for client: '{client_name}', system_id: '{system_id}'.")
        # Create the request record immediately and return the request ID
        request_id = await create_optimization_request_immediately(
            db, client_name, system_id
        )
        logger.info(f"Optimization request '{request_id}' created. Adding background task for processing.")
        # Start the background processing
        background_tasks.add_task(
            process_optimization_in_background,
            client_name, system_id, request_id, ratio_threshold,
            target_license, sap_system_info, role_names
        )

        logger.info(f"Optimization request {request_id} initiated successfully.")
        print(f"Optimization request {request_id} initiated successfully.")

        # Return immediately with the request ID
        return {
            "message": "Optimization request initiated successfully",
            "request_id": request_id,
            "status": "IN_PROGRESS",
            "client_name": client_name,
            "system_id": system_id
        }

    except Exception as e:
        logger.error(f"Failed to initiate optimization request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to initiate optimization request: {str(e)}")



@router.get("/requests", response_model=List[RequestArraySchema])
async def get_all_requests(db: Session = Depends(get_db)):
    """Endpoint to get all requests with proper error handling"""
    logger.info("Received request to retrieve all optimization requests.")

    try:
        logger.debug("Calling get_all_requests_service to fetch data from the database.")
        request = await get_all_requests_service(db)
        if not request:
            logger.warning("No optimization requests were found in the database.")
        else:
            logger.info(f"Successfully retrieved {len(request)} optimization requests.")

        return request
    except Exception as e:
        logger.error(f"Endpoint error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

#
# @router.get("/results/{req_id}", response_model=List[LicenseOptimizationResultSchema])
# def get_results_by_request_id(req_id: str, db: Session = Depends(get_db)):
#     """
#     Retrieves license optimization results for a given request ID.
#     """
#     logger.info(f"Received request for results with REQ_ID: {req_id}")
#
#     # Query the database
#     results = db.query(LicenseOptimizationResult).filter(
#         LicenseOptimizationResult.REQ_ID == req_id
#     ).all()
#
#     # Log the number of results found
#     if not results:
#         logger.warning(f"No results found for REQ_ID: {req_id}")
#         # You could also raise an HTTPException here if you want to
#         # HTTPException(status_code=404, detail="Results not found")
#     else:
#         logger.info(f"Found {len(results)} results for REQ_ID: {req_id}")
#
#     return results

@router.get("/results/{req_id}", response_model=List[LicenseOptimizationResultSchema])
def get_results_by_request_id(req_id: str, db: Session = Depends(get_db)):
    """
    Retrieves license optimization results for a given request ID.
    """
    logger.info(f"Received request for results with REQ_ID: '{req_id}'")

    try:
        logger.debug(f"Querying database for results where REQ_ID == '{req_id}'.")
        results = db.query(LicenseOptimizationResult).filter(
            LicenseOptimizationResult.REQ_ID == req_id
        ).all()

        if not results:
            logger.warning(f"No results found for REQ_ID: '{req_id}'.")
        else:
            logger.info(f"Found {len(results)} results for REQ_ID: '{req_id}'.")
        return results

    except Exception as e:
        logger.error(f"Error retrieving results for REQ_ID: '{req_id}'. Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")



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
    logger.info(f"Received request to get license types for client: '{client_name}', system: '{system_id}'.")
    if not client_name:
        logger.error(f"no client_name was found")
        raise HTTPException(status_code=400, detail="client_name query parameter is required.")
    if not system_id:
        logger.error(f"no system_id was found")
        raise HTTPException(status_code=400, detail="system_id query parameter is required.")
    try:
        logger.debug(f"Calling get_distinct_license_types_db for client: '{client_name}', system: '{system_id}'.")
        licenses = await get_distinct_license_types_db(db, client_name, system_id)
        logger.info(
            f"Successfully retrieved {len(licenses)} distinct license types for client: '{client_name}', system: '{system_id}'.")

        return licenses

    except Exception as e:
        logger.error(
            f"Error retrieving license types for client '{client_name}', system '{system_id}'. Error: {str(e)}",
            exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
