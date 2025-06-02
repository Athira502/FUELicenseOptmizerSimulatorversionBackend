from typing import List

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy.sql import exists
from sqlalchemy import inspect as sqla_inspect, desc

from app.core.logger import logger
from app.models.database import get_db, engine
from app.models.client_sys_release_version import  clientSysReleaseData
from app.models.log_data import logData
from app.service.data_loader_service import (
    load_lice_data_from_xml_upload,
    load_auth_data_from_csv_upload,
    DataLoaderError, load_role_fiori_map_data_from_csv_upload, load_master_derived_role_data_from_csv_upload,
    load_user_role_map_data_from_csv_upload, load_user_data_from_csv_upload
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