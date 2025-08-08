from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List, Tuple
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path, BackgroundTasks
from psycopg2 import ProgrammingError
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.core.logger import setup_logger, get_daily_log_filename

from app.models.database import get_db, SessionLocal
from app.models.dynamic_models import create_auth_obj_field_lic_data, ensure_table_exists, \
    create_role_obj_lic_sim_model, create_user_role_mapping_data_model, create_simulation_result_data, \
    create_lice_data_model
from app.routers.data_loader_router import create_table
from app.routers.example_router import get_simulation_license_classification_pivot_table, \
    get_next_simulation_id_for_table, get_most_restrictive_license
from app.schema.RoleDetailResponse import RoleDetailResponse
from app.schema.SimulationChangePayload import SimulationChangePayload
from app.schema.SpecificRoleDetailsResponseforSim import SpecificRoleDetailsResponseforSim

router = APIRouter(
    prefix="/simulator",
    tags=["Simulation"]
)

logger = setup_logger("app_logger")

@router.get("/auth_object_field_license_data/")
def get_auth_obj_field_lic_data(
        authorization_object: str,
        field: str,
        client_name: str,
        system_name: str,
        db: Session = Depends(get_db)
):
    # Log the start of the request with key parameters
    logger.info(
        f"Retrieving license data for auth object: '{authorization_object}', field: '{field}', client: '{client_name}', system: '{system_name}'")

    try:
        # Create and ensure the dynamic table exists
        logger.debug(f"Creating dynamic model for AuthObjFieldLicData.")
        AuthObjFieldLicDataModel = create_auth_obj_field_lic_data(client_name, system_name)
        ensure_table_exists(db.bind, AuthObjFieldLicDataModel)

        # Log the specific query being executed
        logger.debug(f"Executing query for object: '{authorization_object}' and field: '{field}'.")
        data_records = db.query(AuthObjFieldLicDataModel).filter(
            AuthObjFieldLicDataModel.AUTHORIZATION_OBJECT == authorization_object,
            AuthObjFieldLicDataModel.FIELD == field
        ).all()

        if not data_records:
            # Log a warning if no data is found
            logger.warning(f"No data found for authorization object: '{authorization_object}' and field: '{field}'.")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No data found for AUTHORIZATION_OBJECT: '{authorization_object}' and FIELD: '{field}' for client '{client_name}' and system '{system_name}'"
            )

        # Log successful data retrieval
        logger.info(f"Found {len(data_records)} records for auth object: '{authorization_object}', field: '{field}'.")

        results = [
            {
                "AUTHORIZATION_OBJECT": record.AUTHORIZATION_OBJECT,
                "FIELD": record.FIELD,
                "ACTIVITIY": record.ACTIVITIY,
                "TEXT": record.TEXT,
                "LICENSE": record.LICENSE,
                "UI_TEXT": record.UI_TEXT
            }
            for record in data_records
        ]
        return results

    except Exception as e:
        # Log any unexpected errors with a full traceback
        logger.error(f"Error retrieving auth object field license data. Error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")





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
        logger.debug(f"Creating dynamic model for simulation role data.")
        # Create model and get table name
        DynamicRoleObjLicenseSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        table_name = DynamicRoleObjLicenseSimModel.__tablename__
        logger.debug(f"Using table '{table_name}' for query.")
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
        logger.debug(f"Executing query to fetch role details for role: '{role_name}'.")
        records = db.execute(query, {"role_name": role_name}).fetchall()

        if not records:
            logger.error(f"No records found for role: '{role_name}'")
            raise HTTPException(
                status_code=404,
                detail=f"Role '{role_name}' not found"
            )
        logger.info(f"Found {len(records)} objects for role: '{role_name}'.")
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
        logger.debug("Sorting object details by classification.")
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
        background_tasks: BackgroundTasks,
        db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Creates initial entries for each change in the payload,
    then processes changes in background
    """
    logger.info(f"Received request to apply simulation changes for client: '{client_name}', system: '{system_name}'. Number of changes: {len(changes)}")
    try:
        logger.debug("Creating dynamic models and ensuring table exists.")
        # 1. Setup models
        ResultModel = create_simulation_result_data(client_name, system_name)
        await create_table(db.bind, ResultModel)

        # 2. Create initial records - one per change
        sim_id = get_next_simulation_id_for_table(db, ResultModel)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"Generated new simulation ID: '{sim_id}'. Creating initial database records.")

        # Create a record for each change
        for change in changes:
            record = ResultModel(
                SIMULATION_RUN_ID=sim_id,
                TIMESTAMP=timestamp,
                STATUS="In Progress",
                CLIENT_NAME=client_name,
                SYSTEM_NAME=system_name,
                FUE_REQUIRED="0",
                ROLES_CHANGED=change.role_id,
                ROLE_DESCRIPTION=change.ttext,
                OBJECT=change.object,
                FIELD=change.field_name,
                VALUE_LOW=change.value_low,
                VALUE_HIGH=change.value_high,
                OPERATION=change.action,
                PREV_LICENSE=change.classification,
                CURRENT_LICENSE=None  # Will be updated later
            )
            db.add(record)

        db.commit()
        logger.info(f"Created {len(changes)} initial records for simulation run '{sim_id}'.")

        logger.info(f"Adding background task to process simulation run '{sim_id}'.")
        # 3. Start background processing
        background_tasks.add_task(
            process_simulation_background,
            client_name, system_name, sim_id, changes
        )

        return {
            "simulation_run_id": sim_id,
            "status": "In Progress",
            "timestamp": timestamp,
            "changes_received": len(changes),
            "roles_affected": len({c.role_id for c in changes})
        }

    except Exception as e:
        db.rollback()
        logger.error(f"Simulation initiation failed: {str(e)}")
        raise HTTPException(500, "Simulation initialization failed")


async def process_simulation_background(
        client_name: str,
        system_name: str,
        sim_id: str,
        changes: List[SimulationChangePayload]
):
    logger.info(f"Starting background simulation processing for run '{sim_id}'...")
    """Processes changes and updates existing records using composite key"""
    db = SessionLocal()
    try:
        logger.debug("Initializing dynamic database models for background process.")

        # Initialize models
        ResultModel = create_simulation_result_data(client_name, system_name)
        RoleSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        AuthModel = create_auth_obj_field_lic_data(client_name, system_name)
        await create_table(db.bind, RoleSimModel)
        await create_table(db.bind, AuthModel)

        logger.info(f"Applying comprehensive changes to simulation table for run '{sim_id}'.")

        # 1. Apply changes to simulation table
        records_added, records_changed, records_removed = await apply_comprehensive_changes(
            client_name, system_name, changes, db, RoleSimModel, AuthModel
        )
        logger.info(f"Changes applied. Added: {records_added}, Changed: {records_changed}, Removed: {records_removed}.")
        # 2. Calculate FUE
        logger.debug("Calculating FUE after applying changes.")
        fue_results = await get_simulation_license_classification_pivot_table(
            client_name, system_name, db)
        total_fue = fue_results.get("fue_summary", {}).get("Total FUE Required", 0)
        logger.info(f"Calculated Total FUE required: {total_fue}.")
        logger.debug("Fetching license data to update simulation records.")
        # 3. Get license data for updates
        license_data = db.query(
            RoleSimModel.AGR_NAME,
            RoleSimModel.CLASSIF_S4,
            RoleSimModel.NEW_SIM_LICE
        ).all()

        # Group licenses by role
        license_groups = defaultdict(lambda: {"prev": [], "curr": []})
        for role, prev_lic, curr_lic in license_data:
            if prev_lic:
                license_groups[role]["prev"].append(prev_lic)
            if curr_lic:
                license_groups[role]["curr"].append(curr_lic)

        # 4. Update each change record using composite key
        logger.debug(f"Starting to update {len(changes)} initial records in database.")
        updated_count = 0
        for change in changes:
            licenses = license_groups.get(change.role_id, {})

            # Use the exact same values as when creating the record
            updated_rows = db.query(ResultModel).filter(
                ResultModel.SIMULATION_RUN_ID == sim_id,
                ResultModel.ROLES_CHANGED == change.role_id,
                ResultModel.ROLE_DESCRIPTION == change.ttext,
                ResultModel.OBJECT == change.object,
                ResultModel.FIELD == change.field_name,
                ResultModel.VALUE_LOW == change.value_low,
                ResultModel.STATUS == "In Progress"  # Only update records that are still in progress
            ).update({
                "STATUS": "Completed",
                "FUE_REQUIRED": str(total_fue),
                "CURRENT_LICENSE": get_most_restrictive_license(licenses.get("curr")),
                "TIMESTAMP": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

            if updated_rows == 0:
                logger.warning(
                    f"No 'In Progress' record found for change: "
                    f"role={change.role_id}, object={change.object}, "
                    f"field={change.field_name}, value_low={change.value_low}"
                )
            else:
                updated_count += updated_rows
                logger.debug(f"Updated {updated_rows} record(s) for change: role={change.role_id}")

        db.commit()
        logger.info(f"Successfully updated {updated_count} out of {len(changes)} simulation records")

        # Verify all records were updated
        remaining_in_progress = db.query(ResultModel).filter(
            ResultModel.SIMULATION_RUN_ID == sim_id,
            ResultModel.STATUS == "In Progress"
        ).count()

        if remaining_in_progress > 0:
            logger.warning(f"{remaining_in_progress} records still in 'In Progress' status")

    except Exception as e:
        db.rollback()
        logger.error(f"Background simulation processing failed: {str(e)}")
        try:
            failed_updates = db.query(ResultModel).filter(
                ResultModel.SIMULATION_RUN_ID == sim_id,
                ResultModel.STATUS == "In Progress"
            ).update({
                "STATUS": "Failed",
                "TIMESTAMP": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            db.commit()
            if failed_updates > 0:
                logger.info(f"Marked {failed_updates} records as 'Failed' due to processing error")
        except Exception as update_error:
            logger.error(f"Failed to update error status: {str(update_error)}")
    finally:
        db.close()

async def apply_comprehensive_changes(
        client_name: str,
        system_name: str,
        changes: List[SimulationChangePayload],
        db: Session,
        RoleSimModel,
        AuthModel
) -> tuple[int, int, int]:
    """
    Apply comprehensive changes logic - this is the CRITICAL missing piece!
    This function processes ALL records, not just the changed ones.
    """
    logger.info(f"Applying comprehensive changes for {client_name}/{system_name}")
    logger.debug(f"Number of incoming changes to apply: {len(changes)}.")

    records_added = 0
    records_changed = 0
    records_removed = 0

    # Get ALL existing records
    logger.debug("Fetching all existing simulation records from the database.")
    all_existing_db_records = db.query(RoleSimModel).all()

    existing_records_map: Dict[Tuple[str, str, str, str, str], Any] = {}
    for rec in all_existing_db_records:
        key = (rec.AGR_NAME, rec.OBJECT, rec.FIELD, rec.LOW, rec.HIGH)
        existing_records_map[key] = rec

    # Create frontend changes map
    frontend_changes_map: Dict[Tuple[str, str, str, str, str], SimulationChangePayload] = {}
    for change in changes:
        key = (change.role_id, change.object, change.field_name, change.value_low, change.value_high)
        frontend_changes_map[key] = change

    # Step 1: Process ALL existing records (CRITICAL!)
    logger.debug("Processing existing records to apply 'Change' and 'Remove' operations.")
    for db_record_key, db_record in existing_records_map.items():
        frontend_change_for_this_record = frontend_changes_map.get(db_record_key)

        if frontend_change_for_this_record:
            change = frontend_change_for_this_record

            if change.action == "Change":
                activity, license = parse_ui_text(change.new_value_ui_text)
                db_record.OPERATION = "Change"
                db_record.NEW_VALUE = activity
                db_record.NEW_SIM_LICE = license
                records_changed += 1

            elif change.action == "Remove":
                db_record.OPERATION = "Remove"
                db_record.NEW_VALUE = None
                db_record.NEW_SIM_LICE = None
                records_removed += 1
            else:
                # No change - copy original values
                db_record.OPERATION = None
                db_record.NEW_VALUE = db_record.LOW
                db_record.NEW_SIM_LICE = db_record.CLASSIF_S4
                records_changed += 1
        else:
            # Record not in frontend changes - copy original values (CRITICAL!)
            db_record.OPERATION = None
            db_record.NEW_VALUE = db_record.LOW
            db_record.NEW_SIM_LICE = db_record.CLASSIF_S4
            records_changed += 1

    # Step 2: Handle "Add" operations
    logger.debug("Processing 'Add' operations for new records.")
    for change in changes:
        lookup_key = (change.role_id, change.object, change.field_name, change.value_low, change.value_high)
        existing_record = existing_records_map.get(lookup_key)

        if change.action == "Add" and not existing_record:
            new_license = get_license_for_add_operation(change.object, change.field_name, change.value_low, db,
                                                        AuthModel)

            new_record = RoleSimModel(
                AGR_NAME=change.role_id,
                OBJECT=change.object,
                FIELD=change.field_name,
                LOW=change.value_low,
                HIGH=change.value_high,
                TTEXT=change.ttext,
                CLASSIF_S4=change.classification,
                OPERATION="Add",
                NEW_VALUE=change.value_low,
                NEW_SIM_LICE=new_license
            )
            db.add(new_record)
            records_added += 1

    db.commit()
    logger.info(f"Applied changes: Added={records_added}, Changed={records_changed}, Removed={records_removed}")

    return records_added, records_changed, records_removed


def get_license_for_add_operation(authorization_object: str, field: str, value_low: str, db: Session, AuthModel) -> str:
    """Get license for add operations"""
    logger.info(f"Attempting to fetch license for authorization_object='{authorization_object}', field='{field}', value_low='{value_low}'")

    try:
        auth_records = db.query(AuthModel).filter(
            AuthModel.AUTHORIZATION_OBJECT == authorization_object,
            AuthModel.FIELD == field,
            AuthModel.ACTIVITIY == value_low
        ).all()

        if auth_records:
            record = auth_records[0]
            license = getattr(record, 'LICENSE', None)
            logger.info(f"Found license for {authorization_object}/{field}/{value_low}: {license}")
            return license
        else:
            logger.warning(f"No license found for {authorization_object}/{field}/{value_low}")
            return None

    except Exception as e:
        logger.error(f"Error fetching license: {str(e)}")
        return None


def parse_ui_text(ui_text: str) -> Tuple[str, str]:
    """Extracts activity and license from UI_TEXT"""
    if not ui_text:
        logger.debug("Received empty UI_TEXT, returning None for activity and license.")
        return None, None
    parts = ui_text.split(';')
    return (parts[0], parts[2]) if len(parts) >= 3 else (None, None)

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
    logger.info(f"Starting get_add_suggestions for client='{client_name}', system='{system_name}', auth_object='{authorization_object}', field='{field}'")
    try:
        AuthObjFieldLicDataModel = create_auth_obj_field_lic_data(client_name, system_name)
        logger.debug(f"Created AuthObjFieldLicDataModel for table: {AuthObjFieldLicDataModel.__tablename__}")

        # Query the auth_obj_field_lic_data table
        auth_records = db.query(AuthObjFieldLicDataModel).filter(
            AuthObjFieldLicDataModel.AUTHORIZATION_OBJECT == authorization_object,
            AuthObjFieldLicDataModel.FIELD == field
        ).all()

        if not auth_records:
            logger.info(f"No records found for auth_object='{authorization_object}' and field='{field}'.")
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
                logger.debug(f"Adding suggestion: value='{record.ACTIVITIY}', license='{license}'")

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
    logger.info(f"Starting get_role_details for client='{client_name}' and system='{system_name}'")

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
        logger.debug(f"Executing SQL query for role details: {role_details_query}")

        role_records = db.execute(role_details_query, execution_options={"timeout": 80}).fetchall()

        if not role_records:
            logger.info("No role records found for the given criteria. Returning empty list.")
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
        logger.info(f"Successfully processed {len(response_data)} role details.")


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
    logger.info(f"Checking if simulation table exists for client='{client_name}', system='{system_name}'")

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
