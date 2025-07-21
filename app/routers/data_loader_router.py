from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import exists
from sqlalchemy import inspect as sqla_inspect, desc, text
from app.core.logger import logger
from app.models.database import get_db, engine
from app.models.client_sys_release_version import  clientSysReleaseData
from app.models.dynamic_models import create_role_lic_summary_data_model, create_user_role_mapping_data_model
from app.models.log_data import logData
from app.service.data_loader_service import (
    load_lice_data_from_xml_upload,
    load_auth_data_from_csv_upload,
    DataLoaderError, load_role_fiori_map_data_from_csv_upload, load_master_derived_role_data_from_csv_upload,
    load_user_role_map_data_from_csv_upload, load_user_role_mapping_from_csv_upload, load_user_data_from_csv_upload,
    load_role_lic_summary_data_from_csv_upload, load_auth_obj_field_lic_data_from_csv_upload
)

router = APIRouter(
    prefix="/data",
    tags=["Data Loading"]
)

async def table_exists(db_engine, table_name: str) -> bool:
    """Checks if a table exists in the database."""
    inspector = sqla_inspect(db_engine)
    return inspector.has_table(table_name)

async def create_table(db_engine, model_class):
    """Creates a table if it doesn't exist."""
    table_name = model_class.__tablename__
    if not await table_exists(db_engine, table_name):
        print(f"Creating table: {table_name}")
        try:
            model_class.__table__.create(bind=db_engine)
            print(f"Table '{table_name}' created successfully.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
            raise  # Re-raise the exception after logging
    else:
        print(f"Table '{table_name}' already exists.")

async def ensure_client_system_info(db: Session, client_name: str, system_name: str, system_release_info: str):
    """Ensures client, system, and release info exists in Z_FUE_CLIENT_SYS_INFO."""
    await create_table(engine, clientSysReleaseData)  # Create the table if it doesn't exist
    await create_table(engine,logData)

    exists_query = db.query(exists().where(
        (clientSysReleaseData.CLIENT_NAME == client_name) &
        (clientSysReleaseData.SYSTEM_NAME == system_name) &
        (clientSysReleaseData.SYSTEM_RELEASE_INFO == system_release_info)
    )).scalar()

    if not exists_query:
        db_entry = clientSysReleaseData(
            CLIENT_NAME=client_name,
            SYSTEM_NAME=system_name,
            SYSTEM_RELEASE_INFO=system_release_info
        )
        db.add(db_entry)
        db.commit()
        db.refresh(db_entry)
        print(f"Added new client/system info: {client_name}, {system_name}, {system_release_info}")
    else:
        print(f"Client/system info already exists: {client_name}, {system_name}, {system_release_info}")



@router.post("/load-license-data")
async def load_license_data_endpoint(
    client_name: str,
    system_name: str,
    system_release_info: str,  # Expect system release info
    xml_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Loads license data from an uploaded XML file for a specific client and system.
    """
    print(f"Received request to load license data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = xml_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_lice_data_from_xml_upload(
            db=db,
            xml_file=xml_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"License data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"})
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading license data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail="Error loading license data:str(e)")
    except Exception as e:
        logger.error(f"Unexpected error loading license data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during license data load")

@router.post("/load-auth-data")
async def load_auth_data_endpoint(
    client_name: str,
    system_name: str,
    system_release_info: str,  # Expect system release info
    csv_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Loads authorization data from an uploaded CSV file for a specific client and system.
    """
    print(f"Received request to load auth data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_auth_data_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"Auth data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"})
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading auth data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading auth data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during auth data load")



 # load_role_fiori_map_data_from_csv_upload
@router.post("/load-role-fiori-map-data")
async def load_role_fiori_map_data_endpoint(
        client_name: str,
        system_name: str,
        system_release_info: str,  # Expect system release info
        csv_file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """
    Loads authorization data from an uploaded CSV file for a specific client and system.
    """
    print(
        f"Received request to load Role fiori map data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_role_fiori_map_data_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"Role fiori map data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"})
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading Role fiori map data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading Role fiori map data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during Role fiori map data load")


# load_master_derived_role_data_from_csv_upload
@router.post("/load-master-derived-role-data")
async def load_master_derived_role_data_endpoint(
        client_name: str,
        system_name: str,
        system_release_info: str,  # Expect system release info
        csv_file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """
    Loads authorization data from an uploaded CSV file for a specific client and system.
    """
    print(
        f"Received request to load master derived role data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_master_derived_role_data_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"master derived role data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"})
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading master derived role data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading master derived role data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during master derived role data load")


# load_user_role_map_data_from_csv_upload
@router.post("/load-user-role-map-data")
async def load_user_role_map_data_endpoint(
        client_name: str,
        system_name: str,
        system_release_info: str,  # Expect system release info
        csv_file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """
    Loads authorization data from an uploaded CSV file for a specific client and system.
    """
    print(
        f"Received request to load user_role_map data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_user_role_map_data_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"user_role_map data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"})
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading user_role_map data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading user_role_map data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during user_role_map data load")

# load_user_data_from_csv_upload
@router.post("/load-user-data")
async def load_user_data_endpoint(
        client_name: str,
        system_name: str,
        system_release_info: str,  # Expect system release info
        csv_file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """
    Loads authorization data from an uploaded CSV file for a specific client and system.
    """
    print(
        f"Received request to load user_data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_user_data_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"user data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"})
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading user data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading user data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during user data load")


@router.get("/latest-log", response_model=List[dict])
async def get_latest_logs(db: Session = Depends(get_db)):
    """
    Retrieves the latest 10 log entries from the Z_FUE_LOG_FILE table.
    """
    try:
        logs = (
            db.query(logData)
            .order_by(desc(logData.TIMESTAMP))
            .limit(15)
            .all()
        )

        if logs:
            return [
                {
                    "timestamp": log.TIMESTAMP,
                    "filename": log.FILENAME,
                    "client_name": log.CLIENT_NAME,
                    "system_name": log.SYSTEM_NAME,
                    "system_release_info": log.SYSTEM_RELEASE_INFO,
                    "status": log.STATUS,
                    "log_data": log.LOG_DATA
                }
                for log in logs
            ]
        else:
            raise HTTPException(status_code=404, detail="No log entries found")
    except Exception as e:
        logger.error(f"Error retrieving log entries: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/load-role-lic-summary-data")
async def load_role_lic_summary_data_endpoint(
        client_name: str,
        system_name: str,
        system_release_info: str,
        csv_file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """
    Loads role license summary data from an uploaded CSV file for a specific client and system.
    """
    print(
        f"Received request to load role lic summary data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress",

    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_role_lic_summary_data_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"Role lic summary data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"}
        )
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading role lic summary data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading role lic summary data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during role lic summary data load")



@router.post("/load-user-role-mapping-data")
async def load_user_role_mapping_data_endpoint(
        client_name: str,
        system_name: str,
        system_release_info: str,  # Expect system release info
        csv_file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """
    Loads user role mapping data from an uploaded CSV file for a specific client and system.
    """
    print(
        f"Received request to load user role mapping data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress",

    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        # Call the correct function for user role mapping data
        result = await load_user_role_mapping_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"User role mapping data load completed: {result}")

        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"}
        )
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading user role mapping data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading user role mapping data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during user role mapping data load")



@router.post("/create-role-obj-lic-simulation-table")
async def create_role_obj_lic_simulation_table_endpoint(
        client_name: str = Query(..., min_length=1),
        system_name: str= Query(..., min_length=1),
        system_release_info: str= Query(..., min_length=1),
        db: Session = Depends(get_db)
):

    print(f"Creating simulation table for client: {client_name}, system: {system_name}")

    await ensure_client_system_info(db, client_name, system_name, system_release_info)

    log_entry = logData(
        FILENAME="role_obj_lic_simulation_table_creation",
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id

    try:
        from app.service.data_loader_service import create_and_populate_role_obj_lic_sim_table

        result = await create_and_populate_role_obj_lic_sim_table(
            db=db,
            client_name=client_name,
            system_name=system_name
        )

        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"}
        )
        db.commit()

        return result

    except Exception as e:
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)}
        )
        db.commit()

        raise HTTPException(
            status_code=500,
            detail=f"Error creating simulation table: {str(e)}"
        )


@router.post("/load-auth-obj-field-lic-data")
async def load_auth_obj_field_lic_data_endpoint(
        client_name: str,
        system_name: str,
        system_release_info: str,
        csv_file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """
    Loads authorization data from an uploaded CSV file for a specific client and system.
    """
    print(
        f"Received request to load auth obj field lic data for client: {client_name}, system: {system_name}, release: {system_release_info}")
    await ensure_client_system_info(db, client_name, system_name, system_release_info)
    filename = csv_file.filename
    log_entry = logData(
        FILENAME=filename,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_name,
        SYSTEM_RELEASE_INFO=system_release_info,
        STATUS="In Progress"
    )
    db.add(log_entry)
    db.commit()
    log_id = log_entry.id
    try:
        result = await load_auth_obj_field_lic_data_from_csv_upload(
            db=db,
            csv_file=csv_file.file,
            client_name=client_name,
            system_name=system_name
        )
        print(f"Auth object field license data load completed: {result}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Success"})
        db.commit()
        return result
    except DataLoaderError as e:
        logger.error(f"Error loading Auth object field license data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": str(e)})
        db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error loading Auth object field license data: {e}")
        db.query(logData).filter(logData.id == log_id).update(
            {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
        db.commit()
        raise HTTPException(status_code=500, detail="Internal server error during Auth object field license data load")



@router.get("/pivot-table/license-classification/")
async def get_license_classification_pivot_table(
        client_name: str,
        system_name: str,
        db: Session = Depends(get_db)
) -> Dict[str, Any]:

    """
    Generate a pivot table showing license classification distribution with user counts.
    Returns data similar to: Users | GB Advanced Use | GC Core Use | GD Self-Service Use | Total
    """
    try:
        DynamicRoleLicSummaryModel = create_role_lic_summary_data_model(client_name, system_name)
        DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)

        table_name_summary = DynamicRoleLicSummaryModel.__tablename__
        table_name_mapping = DynamicUserRoleMappingModel.__tablename__

        pivot_query = text(f"""
            WITH role_licenses AS (
                SELECT
                    u."UNAME" as UNAME,
                    r."TARGET_CLASSIFICATION",
                    CASE
                        WHEN r."TARGET_CLASSIFICATION" = 'GB Advanced Use' THEN 1
                        WHEN r."TARGET_CLASSIFICATION" = 'GC Core Use' THEN 2
                        WHEN r."TARGET_CLASSIFICATION" = 'GD Self-Service Use' THEN 3
                    END AS license_priority
                FROM public."{table_name_mapping}" u
                JOIN public."{table_name_summary}" r
                  ON u."AGR_NAME" = r."ROLE"
                WHERE r."TARGET_CLASSIFICATION" IN ('GB Advanced Use', 'GC Core Use', 'GD Self-Service Use')
            ),
            user_min_license AS (
                SELECT UNAME, MIN(license_priority) AS min_priority
                FROM role_licenses
                WHERE license_priority IS NOT NULL
                GROUP BY UNAME
            ),
            user_license_mapped AS (
                SELECT
                    UNAME,
                    CASE min_priority
                        WHEN 1 THEN 'GB Advanced Use'
                        WHEN 2 THEN 'GC Core Use'
                        WHEN 3 THEN 'GD Self-Service Use'
                    END AS effective_license
                FROM user_min_license
            ),
            per_user_fue AS (
                SELECT
                    UNAME,
                    effective_license,
                    CASE
                        WHEN effective_license = 'GB Advanced Use' THEN 1.0
                        WHEN effective_license = 'GC Core Use' THEN 1.0/5.0
                        WHEN effective_license = 'GD Self-Service Use' THEN 1.0/30.0
                    END AS fue_equivalent
                FROM user_license_mapped
            ),
            summary_data AS (
                SELECT
                    COUNT(CASE WHEN effective_license = 'GB Advanced Use' THEN 1 END) AS gb_advanced_use,
                    COUNT(CASE WHEN effective_license = 'GC Core Use' THEN 1 END) AS gc_core_use,
                    COUNT(CASE WHEN effective_license = 'GD Self-Service Use' THEN 1 END) AS gd_self_service_use,
                    0 AS other_licenses,
                    COUNT(*) AS total,
                    ROUND(SUM(CASE WHEN effective_license = 'GB Advanced Use' THEN 1.0 ELSE 0 END)) AS gb_fue,
                    ROUND(SUM(CASE WHEN effective_license = 'GC Core Use' THEN 1.0/5.0 ELSE 0 END)) AS gc_fue,
                    ROUND(SUM(CASE WHEN effective_license = 'GD Self-Service Use' THEN 1.0/30.0 ELSE 0 END)) AS gd_fue
                FROM per_user_fue
            )
            SELECT * FROM summary_data;
        """)

        result = db.execute(pivot_query).fetchone()

        if not result:
            return {
                "pivot_table": {
                    "Users": {
                        "GB Advanced Use": 0,
                        "GC Core Use": 0,
                        "GD Self-Service Use": 0,
                        "Other": 0,
                        "Total": 0
                    }
                },
                "summary": {
                    "total_users": 0,
                    "total_license_types": 0
                },
                "fue_summary": {
                    "GB Advanced Use FUE": 0,
                    "GC Core Use FUE": 0,
                    "GD Self-Service Use FUE": 0,
                    "Total FUE Required": 0
                }
            }

        pivot_table = {
            "Users": {
                "GB Advanced Use": result.gb_advanced_use,
                "GC Core Use": result.gc_core_use,
                "GD Self-Service Use": result.gd_self_service_use,
                "Other": result.other_licenses,
                "Total": result.total
            }
        }

        fue_summary = {
            "GB Advanced Use FUE": result.gb_fue,
            "GC Core Use FUE": result.gc_fue,
            "GD Self-Service Use FUE": result.gd_fue,
            "Total FUE Required": result.gb_fue + result.gc_fue + result.gd_fue
        }



        logger.info(f"Generated pivot table for client: {client_name}, system: {system_name}")

        return {
            "pivot_table": pivot_table,

            "fue_summary": fue_summary,
            "client_name": client_name,
            "system_name": system_name
        }

    except Exception as e:
        logger.error(f"Error generating pivot table: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error generating pivot table: {str(e)}")












#
# @router.post("/update-license-mapping-vlookup")
# async def update_license_mapping_vlookup_endpoint(
#         client_name: str,
#         system_name: str,
#         system_release_info: str,
#         db: Session = Depends(get_db)
# ):
#     """
#     Updates LICENSE_MAPPED_TO_ROLE column using VLOOKUP logic after user role mapping data is loaded.
#     """
#     print(
#         f"Received request to update license mapping via VLOOKUP for client: {client_name}, system: {system_name}, release: {system_release_info}")
#     await ensure_client_system_info(db, client_name, system_name, system_release_info)
#
#     log_entry = logData(
#         FILENAME="VLOOKUP_UPDATE",
#         CLIENT_NAME=client_name,
#         SYSTEM_NAME=system_name,
#         SYSTEM_RELEASE_INFO=system_release_info,
#         STATUS="In Progress"
#     )
#     db.add(log_entry)
#     db.commit()
#     log_id = log_entry.id
#
#     try:
#         result = await update_license_mapped_to_role_via_vlookup(
#             db=db,
#             client_name=client_name,
#             system_name=system_name
#         )
#         print(f"License mapping VLOOKUP update completed: {result}")
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Success"}
#         )
#         db.commit()
#         return result
#     except DataLoaderError as e:
#         logger.error(f"Error updating license mapping via VLOOKUP: {e}")
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Failed", "LOG_DATA": str(e)})
#         db.commit()
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         logger.error(f"Unexpected error updating license mapping via VLOOKUP: {e}")
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
#         db.commit()
#         raise HTTPException(status_code=500, detail="Internal server error during license mapping VLOOKUP update")


#
# @router.post("/load-role-lic-summary-data")
# async def load_role_lic_summary_data_endpoint(
#         client_name: str,
#         system_name: str,
#         system_release_info: str,
#         csv_file: UploadFile = File(...),
#         db: Session = Depends(get_db)
# ):
#
#     print(
#         f"Received request to load role lic summary data for client: {client_name}, system: {system_name}, release: {system_release_info}")
#     await ensure_client_system_info(db, client_name, system_name, system_release_info)
#     filename = csv_file.filename
#     log_entry = logData(
#         FILENAME=filename,
#         CLIENT_NAME=client_name,
#         SYSTEM_NAME=system_name,
#         SYSTEM_RELEASE_INFO=system_release_info,
#         STATUS="In Progress",
#     )
#     db.add(log_entry)
#     db.commit()
#     log_id = log_entry.id
#
#     try:
#         result = await load_role_lic_summary_data_from_csv_upload(
#             db=db,
#             csv_file=csv_file.file,
#             client_name=client_name,
#             system_name=system_name
#         )
#         print(f"Role lic summary data load completed: {result}")
#
#         print(f"🔍 Checking if VLOOKUP should be triggered after role data load...")
#         try:
#             await check_and_trigger_vlookup(db, client_name, system_name, log_id)
#             print(f"✅ VLOOKUP check completed successfully")
#         except Exception as vlookup_error:
#             print(f"⚠️ VLOOKUP check failed but continuing: {vlookup_error}")
#
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Success"}
#         )
#         db.commit()
#         return result
#
#     except DataLoaderError as e:
#         logger.error(f"Error loading role lic summary data: {e}")
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Failed", "LOG_DATA": str(e)})
#         db.commit()
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         logger.error(f"Unexpected error loading role lic summary data: {e}")
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
#         db.commit()
#         raise HTTPException(status_code=500, detail="Internal server error during role lic summary data load")
#
#
# @router.post("/load-user-role-mapping-data")
# async def load_user_role_mapping_data_endpoint(
#         client_name: str,
#         system_name: str,
#         system_release_info: str,
#         csv_file: UploadFile = File(...),
#         db: Session = Depends(get_db)
# ):
#     """
#     Loads user role mapping data from an uploaded CSV file for a specific client and system.
#     """
#     print(
#         f"Received request to load user role mapping data for client: {client_name}, system: {system_name}, release: {system_release_info}")
#     await ensure_client_system_info(db, client_name, system_name, system_release_info)
#     filename = csv_file.filename
#     log_entry = logData(
#         FILENAME=filename,
#         CLIENT_NAME=client_name,
#         SYSTEM_NAME=system_name,
#         SYSTEM_RELEASE_INFO=system_release_info,
#         STATUS="In Progress",
#     )
#     db.add(log_entry)
#     db.commit()
#     log_id = log_entry.id
#
#     try:
#         # Call the correct function for user role mapping data
#         result = await load_user_role_mapping_from_csv_upload(
#             db=db,
#             csv_file=csv_file.file,
#             client_name=client_name,
#             system_name=system_name
#         )
#         print(f"User role mapping data load completed: {result}")
#
#         # ✅ ADD THIS: Check if both tables are loaded and trigger VLOOKUP
#         print(f"🔍 Checking if VLOOKUP should be triggered after user mapping data load...")
#         try:
#             await check_and_trigger_vlookup(db, client_name, system_name, log_id)
#             print(f"✅ VLOOKUP check completed successfully")
#         except Exception as vlookup_error:
#             print(f"⚠️ VLOOKUP check failed but continuing: {vlookup_error}")
#             # Don't fail the main operation if VLOOKUP fails
#
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Success"}
#         )
#         db.commit()
#         return result
#
#     except DataLoaderError as e:
#         logger.error(f"Error loading user role mapping data: {e}")
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Failed", "LOG_DATA": str(e)})
#         db.commit()
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         logger.error(f"Unexpected error loading user role mapping data: {e}")
#         db.query(logData).filter(logData.id == log_id).update(
#             {"STATUS": "Failed", "LOG_DATA": f"Internal server error: {e}"})
#         db.commit()
#         raise HTTPException(status_code=500, detail="Internal server error during user role mapping data load")
#
#
# async def check_and_trigger_vlookup(db: Session, client_name: str, system_name: str, current_log_id: int):
#     """
#     Checks if both required tables have data and triggers VLOOKUP if they do.
#     """
#     try:
#         print(f"🔍 Starting VLOOKUP check for {client_name}-{system_name}")
#         print(f"📝 Current log ID: {current_log_id}")
#
#         role_lic_summary_exists = await check_role_lic_summary_data_exists(db, client_name, system_name)
#         user_role_mapping_exists = await check_user_role_mapping_data_exists(db, client_name, system_name)
#
#         print(f"📊 Table existence check:")
#         print(f"   - Role Lic Summary exists: {role_lic_summary_exists}")
#         print(f"   - User Role Mapping exists: {user_role_mapping_exists}")
#
#         if role_lic_summary_exists and user_role_mapping_exists:
#             print(f"✅ Both tables loaded for {client_name}-{system_name}. Triggering VLOOKUP...")
#
#             db.query(logData).filter(logData.id == current_log_id).update(
#                 {"LOG_DATA": "Data loaded successfully. Starting auto VLOOKUP..."}
#             )
#             db.commit()
#
#             vlookup_log_entry = logData(
#                 FILENAME="AUTO_VLOOKUP_UPDATE",
#                 CLIENT_NAME=client_name,
#                 SYSTEM_NAME=system_name,
#                 SYSTEM_RELEASE_INFO="",
#                 STATUS="In Progress",
#                 LOG_DATA="Auto-triggered VLOOKUP after both tables loaded"
#             )
#             db.add(vlookup_log_entry)
#             db.commit()
#             vlookup_log_id = vlookup_log_entry.id
#
#             print(f"📝 Created VLOOKUP log entry with ID: {vlookup_log_id}")
#
#             try:
#                 print(f"🔄 Calling update_license_mapped_to_role_via_vlookup...")
#
#                 vlookup_result = await update_license_mapped_to_role_via_vlookup(
#                     db=db,
#                     client_name=client_name,
#                     system_name=system_name
#                 )
#
#                 print(f"✅ Auto VLOOKUP completed: {vlookup_result}")
#
#                 db.query(logData).filter(logData.id == vlookup_log_id).update(
#                     {"STATUS": "Success", "LOG_DATA": f"Auto-triggered after data load. Result: {vlookup_result}"}
#                 )
#
#                 db.query(logData).filter(logData.id == current_log_id).update(
#                     {"LOG_DATA": f"Data loaded and VLOOKUP completed successfully. Result: {vlookup_result}"}
#                 )
#                 db.commit()
#
#                 print(f"✅ All logs updated successfully")
#
#             except Exception as vlookup_error:
#                 print(f"❌ Error during auto VLOOKUP: {vlookup_error}")
#                 print(f"❌ Error type: {type(vlookup_error)}")
#                 import traceback
#                 print(f"❌ Full traceback: {traceback.format_exc()}")
#
#                 # Update VLOOKUP log as failed
#                 db.query(logData).filter(logData.id == vlookup_log_id).update(
#                     {"STATUS": "Failed", "LOG_DATA": f"Auto VLOOKUP failed: {vlookup_error}"}
#                 )
#
#                 # Update original log to show VLOOKUP failed but data load succeeded
#                 db.query(logData).filter(logData.id == current_log_id).update(
#                     {"LOG_DATA": f"Data loaded successfully but auto VLOOKUP failed: {vlookup_error}"}
#                 )
#                 db.commit()
#
#                 # Re-raise the error for visibility
#                 raise vlookup_error
#
#         else:
#             missing_tables = []
#             if not role_lic_summary_exists:
#                 missing_tables.append("Role License Summary")
#             if not user_role_mapping_exists:
#                 missing_tables.append("User Role Mapping")
#
#             print(f"⏳ Waiting for missing tables: {', '.join(missing_tables)}")
#
#             db.query(logData).filter(logData.id == current_log_id).update(
#                 {"LOG_DATA": f"Data loaded. Waiting for {', '.join(missing_tables)} data before triggering VLOOKUP."}
#             )
#             db.commit()
#
#     except Exception as e:
#         print(f"❌ Error checking tables for VLOOKUP trigger: {e}")
#         print(f"❌ Error type: {type(e)}")
#         import traceback
#         print(f"❌ Full traceback: {traceback.format_exc()}")
#
#         # Update log but don't fail the main operation
#         try:
#             db.query(logData).filter(logData.id == current_log_id).update(
#                 {"LOG_DATA": f"Data loaded but error checking for VLOOKUP trigger: {e}"}
#             )
#             db.commit()
#         except:
#             pass
#
#         raise e
#
#
# async def check_role_lic_summary_data_exists(db: Session, client_name: str, system_name: str) -> bool:
#     """
#     Check if role license summary data exists for the given client and system.
#     """
#     try:
#         print(f"🔍 Checking Role Lic Summary data for {client_name}-{system_name}")
#
#         DynamicRoleLicSummaryModel = create_role_lic_summary_data_model(client_name, system_name)
#
#         # Just check if the table has any data - don't filter by CLIENT_NAME/SYSTEM_NAME
#         count = db.query(DynamicRoleLicSummaryModel).count()
#
#         print(f"📊 Role Lic Summary records found: {count}")
#         return count > 0
#
#     except Exception as e:
#         print(f"❌ Error checking role lic summary data existence: {e}")
#         return False
#
#
# async def check_user_role_mapping_data_exists(db: Session, client_name: str, system_name: str) -> bool:
#     """
#     Check if user role mapping data exists for the given client and system.
#     """
#     try:
#         print(f"🔍 Checking User Role Mapping data for {client_name}-{system_name}")
#
#         DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)
#
#         # Just check if the table has any data - don't filter by CLIENT_NAME/SYSTEM_NAME
#         count = db.query(DynamicUserRoleMappingModel).count()
#
#         print(f"📊 User Role Mapping records found: {count}")
#         return count > 0
#
#     except Exception as e:
#         print(f"❌ Error checking user role mapping data existence: {e}")
#         return False


