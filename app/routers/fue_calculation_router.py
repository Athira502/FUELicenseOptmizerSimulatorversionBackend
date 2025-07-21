from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from psycopg2 import ProgrammingError
from sqlalchemy.orm import Session
from sqlalchemy import text
from starlette import status
from app.core.logger import logger
from app.models.database import get_db
from app.models.dynamic_models import (
    create_user_role_mapping_data_model,
    create_lice_data_model, create_role_obj_lic_sim_model
)
from app.schema.RoleDetailResponse import RoleDetailResponse
from app.schema.SpecificRoleDetailsResponse import SpecificRoleDetailsResponse

router = APIRouter(
    prefix="/fue",
    tags=["Fue Calculation"]
)

@router.get("/roles/details/", response_model=List[RoleDetailResponse])
async def get_role_details(
        client_name: str = Query(..., description="Client name for filtering roles."),
        system_name: str = Query(..., description="System name for filtering roles."),
        db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    try:
        DynamicRoleObjLicenseInfoModel = create_lice_data_model(client_name, system_name)
        DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)

        table_name_role_obj_info = DynamicRoleObjLicenseInfoModel.__tablename__
        table_name_mapping = DynamicUserRoleMappingModel.__tablename__

        role_details_query = text(f"""
            WITH RoleAggregates AS (
                SELECT
                    ro."AGR_NAME",
                    MAX(ro."AGR_TEXT") AS description,
                    MAX(ro."AGR_CLASSIF") AS classification,
                    SUM(CASE WHEN ro."CLASSIF_S4" = 'GB Advanced Use' THEN 1 ELSE 0 END) AS gb,
                    SUM(CASE WHEN ro."CLASSIF_S4" = 'GC Core Use' THEN 1 ELSE 0 END) AS gc,
                    SUM(CASE WHEN ro."CLASSIF_S4" = 'GD Self-Service Use' THEN 1 ELSE 0 END) AS gd
                FROM public."{table_name_role_obj_info}" ro
                WHERE ro."AGR_CLASSIF" IN ('GB Advanced Use', 'GC Core Use', 'GD Self-Service Use')
                  AND ro."CLASSIF_S4" IN ('GB Advanced Use', 'GC Core Use', 'GD Self-Service Use')
                GROUP BY ro."AGR_NAME"
            ),
            UserCounts AS (
                SELECT
                    urm."AGR_NAME",
                    COUNT(DISTINCT urm."UNAME") AS assignedUsers
                FROM public."{table_name_mapping}" urm
                GROUP BY urm."AGR_NAME"
            )
            SELECT
                ra."AGR_NAME" AS id,
                ra."AGR_NAME" AS profile,
                ra.description,
                ra.classification,
                COALESCE(uc.assignedUsers, 0) AS assignedUsers,
                ra.gb,
                ra.gc,
                ra.gd
            FROM RoleAggregates ra
            LEFT JOIN UserCounts uc ON ra."AGR_NAME" = uc."AGR_NAME"
            ORDER BY ra."AGR_NAME"
        """)

        role_records = db.execute(role_details_query, execution_options={"timeout": 80}).fetchall()

        if not role_records:
            return []

        return [
            {
                "id": str(record[0]),
                "profile": record[1],
                "description": record[2],
                "classification": record[3],
                "assignedUsers": record[4],
                "gb": record[5],
                "gc": record[6],
                "gd": record[7],
                # "not classified": record[8]
            }
            for record in role_records
        ]

    except ProgrammingError as e:
        logger.error(f"SQL Programming Error fetching role details: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Role details for client '{client_name}' and system '{system_name}' not found or tables do not exist. Please check inputs."
        )
    except Exception as e:
        logger.error(f"Error fetching role details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="An unexpected error occurred while fetching role details.")


@router.get("/role-details/{role_name:path}", response_model=SpecificRoleDetailsResponse)
async def get_specific_role_details(
    role_name: str = Path(..., description="Role name for filtering role details, can contain slashes."),
    client_name: str = Query(..., description="Client name for filtering role details."),
    system_name: str = Query(..., description="System name for filtering role details."),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        DynamicRoleObjLicenseInfoModel = create_lice_data_model(client_name, system_name)
        table_name_role_obj_info = DynamicRoleObjLicenseInfoModel.__tablename__


        role_details_query = text(f"""
            SELECT
                "AGR_NAME",
                "AGR_TEXT",
                "OBJECT",
                "CLASSIF_S4",
                "FIELD",
                "LOW",
                "HIGH",
                "TTEXT"
            FROM public."{table_name_role_obj_info}"
            WHERE "AGR_NAME" = :role_name
            ORDER BY "OBJECT", "FIELD";
        """)

        records = db.execute(role_details_query, {"role_name": role_name}).fetchall()

        if not records:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Details for role '{role_name}' not found for client '{client_name}' and system '{system_name}'."
            )

        fetched_role_name = records[0][0]
        role_description = records[0][1]

        object_details = []
        for record in records:
            object_details.append({
                "object": record[2],
                "classification": record[3],
                "fieldName": record[4],
                "valueLow": record[5],
                "valueHigh": record[6],
                "ttext": record[7]
            })

        return {
            "roleName": fetched_role_name,
            "roleDescription": role_description,
            "objectDetails": object_details
        }

    except ProgrammingError as e:
        logger.error(f"SQL Programming Error fetching specific role details: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data tables for client '{client_name}' and system '{system_name}' not found for role details. Please verify inputs."
        )
    except Exception as e:
        logger.error(f"Error fetching specific role details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while fetching role details.")

#
# def get_materialized_view_name(client_name: str, system_name: str) -> str:
#     """
#     Generates a consistent database object name (for a view or materialized view)
#     based on client and system.
#     """
#     client_part = client_name.replace(" ", "_").replace("-", "_").upper()
#     system_part = system_name.replace(" ", "_").replace("-", "_").upper()
#     return f"Z_FUE_{client_part}_{system_part}_ROLE_SUMMARY_MVIEW" # Or _VIEW if you change helper
#
#
# @router.get("/roles/details/", response_model=List[RoleDetailResponse])
# async def get_role_details(
#         client_name: str = Query(..., description="Client name for filtering roles."),
#         system_name: str = Query(..., description="System name for filtering roles."),
#         db: Session = Depends(get_db)
# ) -> List[Dict[str, Any]]:
#     """
#     Fetches aggregated role details from a pre-defined database view.
#     The view must exist and be named according to the get_materialized_view_name helper.
#     """
#     try:
#         # Dynamically determine the name of the view to query
#         view_name = get_materialized_view_name(client_name, system_name)  # Using the same helper
#         logger.info(f"Attempting to fetch role details from view: public.\"{view_name}\" "
#                     f"for client: {client_name}, system: {system_name}")
#
#         # The query is very simple: just select from the view by its name
#         role_details_query = text(f"""
#             SELECT
#                 id,
#                 profile,
#                 description,
#                 classification,
#                 "assignedUsers",
#                 gb,
#                 gc,
#                 gd
#             FROM public."{view_name}"
#             ORDER BY id;
#         """)
#
#         role_records = db.execute(role_details_query, execution_options={"timeout": 80}).fetchall()
#
#         if not role_records:
#             logger.info(f"No records found in view public.\"{view_name}\". Returning empty list.")
#             return []
#
#         logger.info(f"Successfully fetched {len(role_records)} records from public.\"{view_name}\".")
#
#         return [
#             {
#                 "id": str(record[0]),
#                 "profile": record[1],
#                 "description": record[2],
#                 "classification": record[3],
#                 "assignedUsers": record[4],
#                 "gb": record[5],
#                 "gc": record[6],
#                 "gd": record[7]
#             }
#             for record in role_records
#         ]
#
#     except Exception as e:
#         logger.error(f"Error fetching role details from View {view_name}: {str(e)}", exc_info=True)
#
#         if "ProgrammingError" in str(type(e)) or "does not exist" in str(e):
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Role summary data for client '{client_name}' and system '{system_name}' not found. "
#                        f"View '{view_name}' might not exist. "
#                        f"Please ensure the view is created in your database for this client/system combination."
#             )
#         else:
#             raise HTTPException(
#                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#                 detail="An unexpected error occurred while fetching role details."
#             )
