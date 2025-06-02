import re # You might not need this if not used elsewhere in the router
from typing import List, Dict
from fastapi import APIRouter, Depends, HTTPException, Query # Import Query
from sqlalchemy.orm import Session
from app.core.logger import logger
from app.models.database import get_db, engine, Base
from app.models.dynamic_models import create_user_data


router = APIRouter(
    prefix="/user_level",
    tags=["User Level"]
)


@router.get("/user-group", response_model=List[Dict[str, str]])
async def fetch_user_group_data(
    client_name: str = Query(..., description="Client Name (e.g., FUJI)"), # Add client_name as query param
    system_id: str = Query(..., description="System ID (e.g., S4HANA)"),   # Add system_id as query param
    db: Session = Depends(get_db)
):
    """Fetches unique user group names for a specific client and system."""
    try:
        DynamicUserDataModel = create_user_data(client_name, system_id)
        user_groups = db.query(DynamicUserDataModel.USER_GROUP) .filter(DynamicUserDataModel.USER_GROUP.isnot(None),DynamicUserDataModel.USER_GROUP != '' ) .distinct().all()
        return [{"user_groups": user_group.USER_GROUP} for user_group in user_groups]

    except Exception as e:
        logger.error(f"Error fetching user group names for client {client_name}, system {system_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch user group names: {e}")



@router.get("/user-group/{user_group_name}/licenses", response_model=List[Dict[str, str]])
async def fetch_license_type_by_user_group(
    user_group_name: str, # This is a path parameter
    client_name: str = Query(..., description="Client Name (e.g., FUJI)"),
    system_id: str = Query(..., description="System ID (e.g., S4HANA)"),
    db: Session = Depends(get_db)
):
    """Fetches unique license types (TARGET_CLASSIFICATION) for a given user group, client, and system."""
    try:
        DynamicUserDataModel = create_user_data(client_name, system_id)
        license_types = db.query(DynamicUserDataModel.TARGET_CLASSIFICATION).filter(DynamicUserDataModel.USER_GROUP == user_group_name).distinct().all()
        return [{"license_types": lic_type.TARGET_CLASSIFICATION} for lic_type in license_types]
    except Exception as e:
        logger.error(f"Error fetching license_types for user_group {user_group_name} in client {client_name}, system {system_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch license_types for user_group {user_group_name}: {e}")



#
# @router.get("/{user}/licenses", response_model=List[Dict[str, str]])
# async def fetchlicense_type_acc_user_id(
#     user: str,
#     client_name: str = Query(..., description="Client Name (e.g., FUJI)"),
#     system_id: str = Query(..., description="System ID (e.g., S4HANA)"),
#     db: Session = Depends(get_db)
# ):
#     """Fetches unique user group names for a specific client and system."""
#     try:
#         DynamicUserDataModel = create_user_data(client_name, system_id)
#         license_types = db.query(DynamicUserDataModel.TARGET_LICENSE) .filter(DynamicUserDataModel.USER==user) .distinct().all()
#         return [{"license_types": lic_type.TARGET_CLASSIFICATION} for lic_type in license_types]
#     except Exception as e:
#         logger.error(
#             f"Error fetching license_types for user {user} in client {client_name}, system {system_id}: {e}")
#         raise HTTPException(status_code=500,
#                             detail=f"Failed to fetch license_types for user {user}: {e}")