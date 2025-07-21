from typing import Dict, Any, List, Tuple
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from psycopg2 import ProgrammingError
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.core.logger import logger
from app.models.database import get_db
from app.models.dynamic_models import create_auth_obj_field_lic_data, ensure_table_exists, \
    create_role_obj_lic_sim_model, create_user_role_mapping_data_model, create_simulation_result_data, \
    create_lice_data_model
from app.routers.data_loader_router import create_table
from app.schema.RoleDetailResponse import RoleDetailResponse
from app.schema.SimulationChangePayload import SimulationChangePayload
from app.schema.SpecificRoleDetailsResponseforSim import SpecificRoleDetailsResponseforSim

router = APIRouter(
    prefix="/simulator",
    tags=["Simulation"]
)



@router.get("/auth_object_field_license_data/")
def get_auth_obj_field_lic_data(
    authorization_object: str,
    field: str,
    client_name: str,
    system_name: str,
    db: Session = Depends(get_db)
):
    AuthObjFieldLicDataModel = create_auth_obj_field_lic_data(client_name, system_name)

    ensure_table_exists(db.bind, AuthObjFieldLicDataModel)


    data_records = db.query(AuthObjFieldLicDataModel).filter(
        AuthObjFieldLicDataModel.AUTHORIZATION_OBJECT == authorization_object,
        AuthObjFieldLicDataModel.FIELD == field
    ).all()

    if not data_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data found for AUTHORIZATION_OBJECT: '{authorization_object}' and FIELD: '{field}' for client '{client_name}' and system '{system_name}'"
        )

    results = []
    for record in data_records:
        results.append({
            "AUTHORIZATION_OBJECT": record.AUTHORIZATION_OBJECT,
            "FIELD": record.FIELD,
            "ACTIVITIY": record.ACTIVITIY,
            "TEXT": record.TEXT,
            "LICENSE": record.LICENSE,
            "UI_TEXT": record.UI_TEXT
        })

    return results



CLASSIFICATION_ORDER = {
    "GB Advanced Use": 1,
    "GC Core Use": 2,
    "GD Self-Service Use": 3
}


@router.get("/role-details-for-simulation/{role_name:path}")
async def get_specific_role_details(
        role_name: str = Path(...),
        client_name: str = Query(...),
        system_name: str = Query(...),
        db: Session = Depends(get_db)
):
    try:
        logger.info(f"Processing request for role: '{role_name}'")  # Debug logging

        # Create model and get table name
        DynamicRoleObjLicenseSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        table_name = DynamicRoleObjLicenseSimModel.__tablename__

        # Query - use direct parameter binding for safety
        query = text(f"""
            SELECT
                "AGR_NAME",
                "OBJECT",
                "CLASSIF_S4",
                "FIELD",
                "LOW",
                "HIGH",
                "TTEXT"
            FROM public."{table_name}"
            WHERE "AGR_NAME" = :role_name
            ORDER BY "OBJECT", "FIELD";
        """)

        records = db.execute(query, {"role_name": role_name}).fetchall()

        if not records:
            logger.error(f"No records found for role: '{role_name}'")
            raise HTTPException(
                status_code=404,
                detail=f"Role '{role_name}' not found"
            )

        # Process records
        object_details = []
        for record in records:
            object_details.append({
                "object": record[1],
                "classification": record[2],
                "fieldName": record[3],
                "valueLow": record[4],
                "valueHigh": record[5],
                "ttext": record[6]
            })

        # Sort by classification
        object_details.sort(key=lambda x: CLASSIFICATION_ORDER.get(x["classification"], 999))

        return {
            "roleName": records[0][0],
            "objectDetails": object_details
        }

    except ProgrammingError as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(
            status_code=404,
            detail="Table not found. Verify client and system names."
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )


@router.post("/apply-simulation-changes/")
async def apply_simulation_changes(
        client_name: str,
        system_name: str,
        changes: List[SimulationChangePayload],
        db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Applies a list of simulation changes (add, change, remove, or no change) to the
    Z_FUE_{client_name}_{system_name}_ROLE_OBJ_LIC_SIM table.
    This version updates ALL records in the table for the given client/system.
    """
    logger.info(
        f"Applying simulation changes for client: {client_name}, system: {system_name}. Total changes from frontend: {len(changes)}")

    try:
        DynamicRoleObjLicSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        AuthObjFieldLicDataModel = create_auth_obj_field_lic_data(client_name, system_name)

        from app.routers.data_loader_router import create_table
        await create_table(db.bind, DynamicRoleObjLicSimModel)
        await create_table(db.bind, AuthObjFieldLicDataModel)

        records_added = 0
        records_changed = 0
        records_removed = 0

        all_existing_db_records = db.query(DynamicRoleObjLicSimModel).all()

        existing_records_map: Dict[Tuple[str, str, str, str, str], Any] = {}
        for rec in all_existing_db_records:
            key = (rec.AGR_NAME, rec.OBJECT, rec.FIELD, rec.LOW, rec.HIGH)
            existing_records_map[key] = rec

        frontend_changes_map: Dict[Tuple[str, str, str, str, str], SimulationChangePayload] = {}
        for change in changes:
            key = (change.role_id, change.object, change.field_name, change.value_low, change.value_high)
            frontend_changes_map[key] = change

        # Step 1: Process existing records
        for db_record_key, db_record in existing_records_map.items():
            frontend_change_for_this_record = frontend_changes_map.get(db_record_key)

            if frontend_change_for_this_record:
                change = frontend_change_for_this_record

                activity = None
                license = None
                if change.new_value_ui_text:
                    parts = change.new_value_ui_text.split(';')
                    if len(parts) >= 3:
                        activity = parts[0]
                        license = parts[2]
                    else:
                        logger.warning(
                            f"Invalid UI_TEXT format for explicit change: {change.new_value_ui_text}. Expected 'ACTIVITY;TEXT;LICENSE'.")

                if change.action == "Change":
                    db_record.OPERATION = "Change"
                    db_record.NEW_VALUE = activity
                    db_record.NEW_SIM_LICE = license
                    records_changed += 1
                    logger.info(f"Updating record for explicit change: {db_record.AGR_NAME} - {db_record.OBJECT}")

                elif change.action == "Remove":
                    db_record.OPERATION = "Remove"
                    db_record.NEW_VALUE = None
                    db_record.NEW_SIM_LICE = None
                    records_removed += 1
                    logger.info(f"Marking record for explicit removal: {db_record.AGR_NAME} - {db_record.OBJECT}")

                elif change.action is None:
                    db_record.OPERATION = None
                    db_record.NEW_VALUE = db_record.LOW
                    db_record.NEW_SIM_LICE = db_record.CLASSIF_S4
                    records_changed += 1
                    logger.info(
                        f"Copying original values for explicitly sent but unchanged record: {db_record.AGR_NAME} - {db_record.OBJECT}")

            else:
                db_record.OPERATION = None
                db_record.NEW_VALUE = db_record.LOW
                db_record.NEW_SIM_LICE = db_record.CLASSIF_S4
                records_changed += 1
                logger.info(
                    f"Copying original values for implicitly unchanged record: {db_record.AGR_NAME} - {db_record.OBJECT}")

        # Step 2: Enhanced helper function to get license from auth_obj_field_lic_data
        def get_license_for_add_operation(authorization_object: str, field: str, value_low: str) -> tuple[str, str]:
            """
            Fetch license and UI_TEXT from auth_obj_field_lic_data table based on authorization_object, field, and matching activity
            Returns: (license, ui_text)
            """
            try:
                # Query the auth_obj_field_lic_data table
                # Note: Using ACTIVITIY (with the typo as shown in your screenshot)
                auth_records = db.query(AuthObjFieldLicDataModel).filter(
                    AuthObjFieldLicDataModel.AUTHORIZATION_OBJECT == authorization_object,
                    AuthObjFieldLicDataModel.FIELD == field,
                    AuthObjFieldLicDataModel.ACTIVITIY == value_low  # Match the activity with value_low
                ).all()

                if auth_records:
                    # Return the license and UI_TEXT from the first matching record
                    record = auth_records[0]
                    license = record.LICENSE if hasattr(record, 'LICENSE') else None
                    ui_text = record.UI_TEXT if hasattr(record, 'UI_TEXT') else None
                    logger.info(f"Found license for {authorization_object}/{field}/{value_low}: {license}")
                    return license, ui_text
                else:
                    logger.warning(f"No license found for {authorization_object}/{field}/{value_low}")
                    return None, None

            except Exception as e:
                logger.error(f"Error fetching license for {authorization_object}/{field}/{value_low}: {str(e)}")
                return None, None

        # Step 3: Handle "Add" operations (new records from frontend that don't exist in DB)
        for change in changes:
            lookup_key_from_payload = (
                change.role_id,
                change.object,
                change.field_name,
                change.value_low,
                change.value_high,
            )
            existing_record_in_db = existing_records_map.get(lookup_key_from_payload)

            if change.action == "Add" and not existing_record_in_db:
                # For Add operations, use value_low as NEW_VALUE and fetch license from auth_obj_field_lic_data
                new_value = change.value_low
                new_license, ui_text = get_license_for_add_operation(change.object, change.field_name, change.value_low)

                # If we couldn't fetch license from auth table, try to parse from new_value_ui_text as fallback
                if not new_license and change.new_value_ui_text:
                    parts = change.new_value_ui_text.split(';')
                    if len(parts) >= 3:
                        new_license = parts[2]
                        logger.info(f"Using fallback license from UI_TEXT: {new_license}")

                # If still no license, log warning but continue
                if not new_license:
                    logger.warning(
                        f"No license found for Add operation: {change.object}/{change.field_name}/{change.value_low}")

                new_record = DynamicRoleObjLicSimModel(
                    AGR_NAME=change.role_id,
                    OBJECT=change.object,
                    FIELD=change.field_name,
                    LOW=change.value_low,
                    HIGH=change.value_high,
                    TTEXT=change.ttext,
                    CLASSIF_S4=change.classification,
                    OPERATION="Add",
                    NEW_VALUE=new_value,
                    NEW_SIM_LICE=new_license
                )
                db.add(new_record)
                records_added += 1
                logger.info(
                    f"Adding new record from frontend: {change.role_id} - {change.object}, NEW_VALUE: {new_value}, NEW_SIM_LICE: {new_license}")

            elif change.action == "Add" and existing_record_in_db:
                # This case is handled in Step 1 for existing records, just log here.
                logger.warning(
                    f"Frontend sent 'Add' for an existing record. It was updated in Step 1: {change.role_id} - {change.object}")

        db.commit()  # Commit all changes at once

        return {
            "message": "Simulation changes applied successfully to all relevant records.",
            "added_records": records_added,
            "changed_records": records_changed,
            "removed_records": records_removed,
            "client_name": client_name,
            "system_name": system_name,
        }

    except Exception as e:
        db.rollback()  # Rollback in case of any error
        logger.error(f"Error applying simulation changes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error applying simulation changes: {str(e)}")


@router.get("/get-add-suggestions/")
def get_add_suggestions(
        authorization_object: str,
        field: str,
        client_name: str,
        system_name: str,
        db: Session = Depends(get_db)
):
    """
    Get suggestions for Add operations including both activity and license
    """
    try:
        AuthObjFieldLicDataModel = create_auth_obj_field_lic_data(client_name, system_name)

        # Query the auth_obj_field_lic_data table
        auth_records = db.query(AuthObjFieldLicDataModel).filter(
            AuthObjFieldLicDataModel.AUTHORIZATION_OBJECT == authorization_object,
            AuthObjFieldLicDataModel.FIELD == field
        ).all()

        if not auth_records:
            return []

        suggestions = []
        for record in auth_records:
            if hasattr(record, 'UI_TEXT') and record.UI_TEXT:
                suggestions.append({
                    "value": record.ACTIVITIY,  # The activity value
                    "license": record.LICENSE,  # The corresponding license
                    "ui_text": record.UI_TEXT,  # The full UI text
                    "text": record.TEXT if hasattr(record, 'TEXT') else ""
                })

        return suggestions

    except Exception as e:
        logger.error(f"Error getting add suggestions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting add suggestions: {str(e)}")



@router.get("/roles_for_sim/details/", response_model=List[RoleDetailResponse])
async def get_role_details(
        client_name: str = Query(..., description="Client name for filtering roles."),
        system_name: str = Query(..., description="System name for filtering roles."),
        db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    try:
        # Check if simulation table exists and create/populate if needed
        await ensure_simulation_table_exists(db, client_name, system_name)

        DynamicRoleObjLicenseSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)
        DynamicRoleObjLicModel=create_lice_data_model(client_name, system_name)

        table_name_role_obj_info = DynamicRoleObjLicModel.__tablename__
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
                "gd": record[7]
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


async def ensure_simulation_table_exists(db: Session, client_name: str, system_name: str):
    """
    Check if simulation table exists for the given client and system.
    If not, create the table and populate it with data.
    If exists but empty, populate it with data.
    """
    try:
        # Get the simulation model
        DynamicRoleObjLicSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        table_name = DynamicRoleObjLicSimModel.__tablename__

        # Check if table exists
        table_exists_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            )
        """)

        result = db.execute(table_exists_query, {"table_name": table_name}).fetchone()
        table_exists = result[0] if result else False

        if not table_exists:
            logger.info(f"Simulation table {table_name} does not exist. Creating and populating...")
            # Create table and populate data
            from app.service.data_loader_service import create_and_populate_role_obj_lic_sim_table
            await create_and_populate_role_obj_lic_sim_table(db, client_name, system_name)
            logger.info(f"Successfully created and populated simulation table {table_name}")
        else:
            # Table exists, check if it has data
            record_count = db.query(DynamicRoleObjLicSimModel).count()
            if record_count == 0:
                logger.info(f"Simulation table {table_name} exists but is empty. Populating with data...")
                # Populate data
                from app.service.data_loader_service import create_and_populate_role_obj_lic_sim_table
                await create_and_populate_role_obj_lic_sim_table(db, client_name, system_name)
                logger.info(f"Successfully populated simulation table {table_name}")
            else:
                logger.info(f"Simulation table {table_name} exists with {record_count} records")

    except Exception as e:
        logger.error(f"Error ensuring simulation table exists: {str(e)}", exc_info=True)


        # Don't raise exception here - let the main endpoint handle it
        # This allows the role details endpoint to work even if simulation table creation fails
# @router.get("/role-details-for-simulation/{role_name:path}", response_model=SpecificRoleDetailsResponseforSim)
# async def get_specific_role_details(
#     role_name: str = Path(..., description="Role name for filtering role details, can contain slashes."),
#     client_name: str = Query(..., description="Client name for filtering role details."),
#     system_name: str = Query(..., description="System name for filtering role details."),
#     db: Session = Depends(get_db)
# ) -> Dict[str, Any]:
#     try:
#         DynamicRoleObjLicenseSimModel = create_role_obj_lic_sim_model(client_name, system_name)
#         table_name_role_obj_sim_info = DynamicRoleObjLicenseSimModel.__tablename__
#
#
#         role_details_query = text(f"""
#             SELECT
#                 "AGR_NAME",      -- Index 0
#                 "OBJECT",        -- Index 1
#                 "CLASSIF_S4",    -- Index 2 (Object Classification)
#                 "FIELD",         -- Index 3
#                 "LOW",           -- Index 4
#                 "HIGH",          -- Index 5
#                 "TTEXT"          -- Index 6 (Text for Object/Field)
#             FROM public."{table_name_role_obj_sim_info}"
#             WHERE "AGR_NAME" = :role_name
#             ORDER BY "OBJECT", "FIELD";
#         """)
#
#         records = db.execute(role_details_query, {"role_name": role_name}).fetchall()
#
#         if not records:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"Details for role '{role_name}' not found for client '{client_name}' and system '{system_name}'."
#             )
#
#         fetched_role_name = records[0][0]
#
#         object_details = []
#         for record in records:
#             object_details.append({
#                 "object": record[1],
#                 "classification": record[2],
#                 "fieldName": record[3],
#                 "valueLow": record[4],
#                 "valueHigh": record[5],
#                 "ttext": record[6]
#             })
#
#             def sort_key(item):
#                 classification = item.get("classification")
#
#                 return CLASSIFICATION_ORDER.get(classification, 999)
#
#             object_details.sort(key=sort_key)
#
#
#         return {
#             "roleName": fetched_role_name,
#             "objectDetails": object_details
#         }
#
#     except ProgrammingError as e:
#         logger.error(f"SQL Programming Error fetching specific role details: {str(e)}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail=f"Data tables for client '{client_name}' and system '{system_name}' not found for role details. Please verify inputs."
#         )
#     except Exception as e:
#         logger.error(f"Error fetching specific role details: {str(e)}", exc_info=True)
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while fetching role details.")



# @router.get("/license-classification-simulation/")
# async def get_simulation_license_classification_pivot_table(
#     client_name: str,
#     system_name: str,
#     db: Session = Depends(get_db)
# ) -> Dict[str, Any]:
#     """
#     Generate a pivot table showing license classification distribution with user counts,
#     based on the most restrictive license derived from role objects.
#     """
#     logger.info(f"Generating license classification pivot table for client: {client_name}, system: {system_name}")
#
#     try:
#         DynamicRoleObjLicSimModel = create_role_obj_lic_sim_model(client_name, system_name)
#         DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)
#         # DynamicSimulatorResultModel =create_simulation_result_data(client_name,system_name)
#
#         table_name_sim_model = DynamicRoleObjLicSimModel.__tablename__
#         table_name_mapping = DynamicUserRoleMappingModel.__tablename__
#         # table_name_sim_result=DynamicSimulatorResultModel.__tablename__
#
#         pivot_query = text(f"""
#             WITH role_license_priority AS (
#                 -- Step 1: Assign license priority to objects in each role
#                 SELECT
#                     "AGR_NAME" AS role,
#                     CASE
#                         WHEN "NEW_SIM_LICE" = 'GB Advanced Use' THEN 1
#                         WHEN "NEW_SIM_LICE" = 'GC Core Use' THEN 2
#                         WHEN "NEW_SIM_LICE" = 'GD Self-Service Use' THEN 3
#                         ELSE 99
#                     END AS license_priority,
#                     "NEW_SIM_LICE" AS license_type
#                 FROM public."{table_name_sim_model}"
#                 WHERE "NEW_SIM_LICE" IN ('GB Advanced Use', 'GC Core Use', 'GD Self-Service Use')
#             ),
#             role_most_restrictive_license AS (
#                 -- Step 2: Find the most restrictive license per role (lowest priority number)
#                 SELECT
#                     role,
#                     MIN(license_priority) AS min_priority
#                 FROM role_license_priority
#                 GROUP BY role
#             ),
#             role_license_classification AS (
#                 -- Step 3: Convert priority back to license name
#                 SELECT
#                     r.role,
#                     CASE r.min_priority
#                         WHEN 1 THEN 'GB Advanced Use'
#                         WHEN 2 THEN 'GC Core Use'
#                         WHEN 3 THEN 'GD Self-Service Use'
#                         ELSE 'Other'
#                     END AS role_license
#                 FROM role_most_restrictive_license r
#             ),
#             user_roles_with_license AS (
#                 -- Step 4: Join user-role mapping with license per role
#                 SELECT
#                     urm."UNAME",
#                     urm."AGR_NAME" AS role,
#                     rlc.role_license,
#                     CASE rlc.role_license
#                         WHEN 'GB Advanced Use' THEN 1
#                         WHEN 'GC Core Use' THEN 2
#                         WHEN 'GD Self-Service Use' THEN 3
#                         ELSE 99
#                     END AS license_priority
#                 FROM public."{table_name_mapping}" urm
#                 JOIN role_license_classification rlc
#                     ON urm."AGR_NAME" = rlc.role
#             ),
#             user_effective_license AS (
#                 -- Step 5: Determine user's most restrictive license across all their roles
#                 SELECT
#                     "UNAME",
#                     MIN(license_priority) AS effective_priority
#                 FROM user_roles_with_license
#                 GROUP BY "UNAME"
#             ),
#             user_final_license AS (
#                 -- Step 6: Convert priority to license name
#                 SELECT
#                     "UNAME",
#                     CASE effective_priority
#                         WHEN 1 THEN 'GB Advanced Use'
#                         WHEN 2 THEN 'GC Core Use'
#                         WHEN 3 THEN 'GD Self-Service Use'
#                         ELSE 'Other'
#                     END AS final_license
#                 FROM user_effective_license
#             ),
#             fue_summary AS (
#                 -- Step 7: Aggregate for FUE calculation
#                 SELECT
#                     COUNT(*) AS total_users,
#                     COUNT(CASE WHEN final_license = 'GB Advanced Use' THEN 1 END) AS gb_users,
#                     COUNT(CASE WHEN final_license = 'GC Core Use' THEN 1 END) AS gc_users,
#                     COUNT(CASE WHEN final_license = 'GD Self-Service Use' THEN 1 END) AS gd_users,
#                     COUNT(CASE WHEN final_license = 'Other' THEN 1 END) AS other_users,
#                     CEIL(SUM(CASE WHEN final_license = 'GB Advanced Use' THEN 1.0 ELSE 0 END)) AS gb_fue,
#                     CEIL(SUM(CASE WHEN final_license = 'GC Core Use' THEN 1.0 / 5 ELSE 0 END)) AS gc_fue,
#                     CEIL(SUM(CASE WHEN final_license = 'GD Self-Service Use' THEN 1.0 / 30 ELSE 0 END)) AS gd_fue,
#                     CEIL(SUM(CASE
#             WHEN final_license = 'GB Advanced Use' THEN 1.0
#             WHEN final_license = 'GC Core Use' THEN 1.0 / 5
#             WHEN final_license = 'GD Self-Service Use' THEN 1.0 / 30
#             ELSE 0.0
#         END)) AS total_fue_required
#                 FROM user_final_license
#             )
#             SELECT * FROM fue_summary;
#         """)
#
#         result = db.execute(pivot_query).fetchone()
#
#         if not result:
#             return {
#                 "pivot_table": {
#                     "Users": {
#                         "GB Advanced Use": 0,
#                         "GC Core Use": 0,
#                         "GD Self-Service Use": 0,
#                         "Other": 0,
#                         "Total": 0
#                     }
#                 },
#                 "fue_summary": {
#                     "GB Advanced Use FUE": 0,
#                     "GC Core Use FUE": 0,
#                     "GD Self-Service Use FUE": 0,
#                     "Total FUE Required": 0
#                 },
#                 "client_name": client_name,
#                 "system_name": system_name
#             }
#
#         pivot_table = {
#             "Users": {
#                 "GB Advanced Use": result.gb_users,
#                 "GC Core Use": result.gc_users,
#                 "GD Self-Service Use": result.gd_users,
#                 "Other": result.other_users,
#                 "Total": result.total_users
#             }
#         }
#
#         fue_summary = {
#             "GB Advanced Use FUE": result.gb_fue,
#             "GC Core Use FUE": result.gc_fue,
#             "GD Self-Service Use FUE": result.gd_fue,
#             "Total FUE Required": result.total_fue_required
#         }
#
#         logger.info(f"Generated pivot table for client: {client_name}, system: {system_name}")
#
#         return {
#             "pivot_table": pivot_table,
#             "fue_summary": fue_summary,
#             "client_name": client_name,
#             "system_name": system_name
#         }
#
#     except Exception as e:
#         logger.error(f"Error generating pivot table: {str(e)}", exc_info=True)
#         raise HTTPException(status_code=500, detail=f"Error generating pivot table: {str(e)}")
#
#
