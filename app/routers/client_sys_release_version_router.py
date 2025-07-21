
from sqlalchemy import inspect as sqla_inspect, text
from typing import List, Dict
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from fastapi.responses import StreamingResponse
import pandas as pd
from io import StringIO
from app.core.logger import logger
from app.models.database import get_db, engine, Base
from app.models.client_sys_release_version import clientSysReleaseData
from app.models.dynamic_models import (
    get_lice_data_tablename,
    get_auth_data_tablename,
    get_role_fiori_data_tablename,
    get_role_master_derived_data_tablename,
    get_user_data_tablename,
    get_user_role_data_tablename, get_role_lic_summary_data_tablename, get_user_role_mapping_data_tablename,
    get_role_obj_lic_sim_tablename, get_auth_obj_field_lic_data_tablename, get_simulation_result_tablename,
)

router = APIRouter(
    prefix="/manage-data",
    tags=["Manage Data"]
)

async def table_exists(db_engine, table_name: str) -> bool:
    """Checks if a table exists in the database."""
    inspector = sqla_inspect(db_engine)
    return inspector.has_table(table_name)

@router.get("/clients", response_model=List[Dict[str, str]])
async def fetch_client_data(db: Session = Depends(get_db)):
    """Fetches unique client names."""
    try:
        clients = db.query(clientSysReleaseData.CLIENT_NAME).distinct().all()
        return [{"client_name": client.CLIENT_NAME} for client in clients]
    except Exception as e:
        logger.error(f"Error fetching client names: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch client names")

@router.get("/systems/{client_name}", response_model=List[Dict[str, str]])
async def fetch_systems_by_client(client_name: str, db: Session = Depends(get_db)):
    """Fetches unique system names for a given client."""
    try:
        systems = db.query(clientSysReleaseData.SYSTEM_NAME).filter(clientSysReleaseData.CLIENT_NAME == client_name).distinct().all()
        return [{"system_name": system.SYSTEM_NAME} for system in systems]
    except Exception as e:
        logger.error(f"Error fetching system names for client {client_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch system names for client {client_name}")

@router.get("/tables/{client_name}/{system_name}", response_model=List[str])
async def get_tables_for_client_system(client_name: str, system_name: str):
    """Returns a list of existing table names for a given client and system."""
    potential_table_names = [
        get_lice_data_tablename(client_name, system_name),
        get_auth_data_tablename(client_name, system_name),
        get_role_fiori_data_tablename(client_name, system_name),
        get_role_master_derived_data_tablename(client_name, system_name),
        get_user_data_tablename(client_name, system_name),
        get_user_role_data_tablename(client_name, system_name),
        get_role_lic_summary_data_tablename(client_name, system_name),
        get_user_role_mapping_data_tablename(client_name, system_name),
        get_role_obj_lic_sim_tablename(client_name, system_name),
        get_auth_obj_field_lic_data_tablename(client_name, system_name),
        get_simulation_result_tablename(client_name, system_name)
    ]
    inspector = sqla_inspect(engine)
    existing_tables = inspector.get_table_names()
    return [table for table in potential_table_names if table in existing_tables]

@router.get("/download/{client_name}/{system_name}/{table_name}")
async def download_table_data(client_name: str, system_name: str, table_name: str, db: Session = Depends(get_db)):
    """Downloads data from a specified table for a client and system as CSV."""
    logger.info(f"download_table_data called with: client_name={client_name}, system_name={system_name}, table_name={table_name}")

    if not await table_exists(engine, table_name):
        error_message = f"Table '{table_name}' not found"
        logger.error(error_message)
        raise HTTPException(status_code=404, detail=error_message)

    try:
        query_string = f'SELECT * FROM public."{table_name}"'
        logger.info(f"Executing query: {query_string}")
        query = text(query_string)

        result_proxy = db.execute(query)
        result = result_proxy.fetchall()
        columns = result_proxy.keys()  # Get column names from metadata

        if not result:
            message = f"No data found in table '{table_name}'"
            logger.info(message)
            return JSONResponse(content={"message": message}, status_code=200)

        df = pd.DataFrame(result, columns=columns)
        csv_output = StringIO()
        df.to_csv(csv_output, index=False)

        response = StreamingResponse(
            iter([csv_output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={table_name}.csv"},
        )
        logger.info("Returning StreamingResponse")
        return response

    except Exception as e:
        error_message = f"Error downloading data from {table_name}: {e}"
        logger.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)



@router.delete("/delete/{client_name}/{system_name}/{table_name}")
async def truncate_table(client_name: str, system_name: str, table_name: str, db: Session = Depends(get_db)):
    """Truncates (deletes all data from) a specified table for a client and system."""
    if not await table_exists(engine, table_name):
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    try:
        query = text(f'DROP TABLE public."{table_name}"')
        db.execute(query)
        db.commit()
        return JSONResponse(content={"message": f"Table '{table_name}' deleted successfully"}, status_code=200)
    except Exception as e:
        logger.error(f"Error deleting table {table_name}: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Failed to delete table")
