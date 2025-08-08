from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.core.logger import setup_logger
from app.models.database import get_db
from app.models.dynamic_models import (
    create_role_obj_lic_sim_model, create_simulation_result_data,
    create_user_role_mapping_data_model
)
from app.routers.data_loader_router import create_table

router = APIRouter(
    prefix="/simulation_result",
    tags=["Simulation Result"]
)

logger = setup_logger("app_logger")

LICENSE_RESTRICTIVENESS_ORDER = {
    'GB Advanced Use': 3,
    'GC Core Use': 2,
    'GD Self-Service Use': 1,
    'N/A': 0,
    None: 0
}


def get_most_restrictive_license(licenses: list[str]) -> str | None:
    """
    Given a list of licenses, returns the most restrictive one based on a predefined order.
    """
    if not licenses:
        return None

    most_restrictive = None
    max_restrictiveness_score = -1
    logger.info(f"calculating the most restrictive license")
    for lic in licenses:
        score = LICENSE_RESTRICTIVENESS_ORDER.get(lic, 0)  # Default to 0 for unknown licenses
        if score > max_restrictiveness_score:
            max_restrictiveness_score = score
            most_restrictive = lic
    return most_restrictive


async def get_simulation_license_classification_pivot_table(
    client_name: str,
    system_name: str,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Generate a pivot table showing license classification distribution with user counts,
    based on the most restrictive license derived from role objects.
    """
    logger.info(f"Generating license classification pivot table for client: {client_name}, system: {system_name}")

    try:
        logger.debug(f"Creating dynamic models for simulation results and user role mapping.")
        DynamicRoleObjLicSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)
        logger.debug(f"Ensuring tables '{DynamicRoleObjLicSimModel.__tablename__}' and '{DynamicUserRoleMappingModel.__tablename__}' exist.")
        await create_table(db.bind, DynamicRoleObjLicSimModel)
        await create_table(db.bind, DynamicUserRoleMappingModel)

        table_name_sim_model = DynamicRoleObjLicSimModel.__tablename__
        table_name_mapping = DynamicUserRoleMappingModel.__tablename__

        pivot_query = text(f"""
           WITH role_license_classification AS (
    SELECT DISTINCT ON ("AGR_NAME")
        "AGR_NAME" AS role,
        "NEW_SIM_LICE" AS role_license
    FROM public."{table_name_sim_model}"
    WHERE "NEW_SIM_LICE" IN ('GB Advanced Use', 'GC Core Use', 'GD Self-Service Use')
    ORDER BY "AGR_NAME",
        CASE "NEW_SIM_LICE"
            WHEN 'GB Advanced Use' THEN 1
            WHEN 'GC Core Use' THEN 2
            WHEN 'GD Self-Service Use' THEN 3
            ELSE 99
        END
),
user_final_license AS (
    SELECT DISTINCT ON (urm."UNAME")
        urm."UNAME",
        rlc.role_license AS final_license
    FROM public."{table_name_mapping}" urm
    JOIN role_license_classification rlc
        ON urm."AGR_NAME" = rlc.role
    ORDER BY urm."UNAME",
        CASE rlc.role_license
            WHEN 'GB Advanced Use' THEN 1
            WHEN 'GC Core Use' THEN 2
            WHEN 'GD Self-Service Use' THEN 3
            ELSE 99
        END
),
fue_summary AS (
    SELECT
        COUNT(*) AS total_users,
        COUNT(CASE WHEN final_license = 'GB Advanced Use' THEN 1 END) AS gb_users,
        COUNT(CASE WHEN final_license = 'GC Core Use' THEN 1 END) AS gc_users,
        COUNT(CASE WHEN final_license = 'GD Self-Service Use' THEN 1 END) AS gd_users,
        CEIL(SUM(CASE WHEN final_license = 'GB Advanced Use' THEN 1.0 ELSE 0 END)) AS gb_fue,
        CEIL(SUM(CASE WHEN final_license = 'GC Core Use' THEN 1.0 / 5 ELSE 0 END)) AS gc_fue,
        CEIL(SUM(CASE WHEN final_license = 'GD Self-Service Use' THEN 1.0 / 30 ELSE 0 END)) AS gd_fue,
        CEIL(SUM(CASE
            WHEN final_license = 'GB Advanced Use' THEN 1.0
            WHEN final_license = 'GC Core Use' THEN 1.0 / 5
            WHEN final_license = 'GD Self-Service Use' THEN 1.0 / 30
            ELSE 0.0
        END)) AS total_fue_required
    FROM user_final_license
)
SELECT * FROM fue_summary;
        """)
        logger.debug(f"Executing pivot query:\n{pivot_query.text}")
        result = db.execute(pivot_query).fetchone()

        if not result:
            logger.warning(f"No results found for pivot table query for client: {client_name}, system: {system_name}. Returning empty data.")
            return {
                "pivot_table": {
                    "Users": {
                        "GB Advanced Use": 0,
                        "GC Core Use": 0,
                        "GD Self-Service Use": 0,
                        "Total": 0
                    }
                },
                "fue_summary": {
                    "GB Advanced Use FUE": 0,
                    "GC Core Use FUE": 0,
                    "GD Self-Service Use FUE": 0,
                    "Total FUE Required": 0
                },
                "client_name": client_name,
                "system_name": system_name
            }
        logger.info(f"Successfully generated pivot table. Total Users: {result.total_users}, Total FUE: {result.total_fue_required}")
        pivot_table = {
            "Users": {
                "GB Advanced Use": result.gb_users,
                "GC Core Use": result.gc_users,
                "GD Self-Service Use": result.gd_users,
                "Total": result.total_users
            }
        }

        fue_summary = {
            "GB Advanced Use FUE": result.gb_fue,
            "GC Core Use FUE": result.gc_fue,
            "GD Self-Service Use FUE": result.gd_fue,
            "Total FUE Required": result.total_fue_required
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

def get_next_simulation_id_for_table(db: Session, DynamicSimulationResultModel) -> str:
    logger.debug(f"Determining next simulation ID for table: {DynamicSimulationResultModel.__tablename__}")

    try:
        logger.debug("Querying for the latest simulation ID.")
        # Get the highest existing simulation ID from the current table
        latest_record = db.query(DynamicSimulationResultModel.SIMULATION_RUN_ID).filter(
            DynamicSimulationResultModel.SIMULATION_RUN_ID.like('SIM%')
        ).order_by(DynamicSimulationResultModel.SIMULATION_RUN_ID.desc()).first()

        if latest_record and latest_record[0].startswith('SIM'):
            try:
                # Extract the number part and increment
                current_num = int(latest_record[0][3:])  # Remove 'SIM' prefix
                next_num = current_num + 1
                logger.debug(f"Found latest simulation ID '{latest_record[0]}'. Next ID will be 'SIM{next_num}'.")
            except ValueError:
                # If parsing fails, start from 100000
                next_num = 100000
                logger.warning(f"Could not parse simulation ID '{latest_record[0]}'. Falling back to 'SIM{next_num}'.")

        else:
            # No existing records, start from 100000
            next_num = 100000
            logger.info(f"No existing simulation IDs found. Starting with 'SIM{next_num}'.")


        return f"SIM{next_num}"

    except Exception as e:
        logger.error(f"Error in generating next simulation ID. Falling back to 'SIM100000'. Error: {e}", exc_info=True)
        # Fallback to default if any error occurs
        return "SIM100000"


@router.get("/simulation-results/")
async def get_simulation_results(
        client_name: str,
        system_name: str,
        db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get all simulation results for a client and system.
    """
    logger.info(f"Retrieving simulation results for client: {client_name}, system: {system_name}")

    try:
        logger.debug(f"Creating dynamic model for simulation results data.")
        DynamicSimulationResultModel = create_simulation_result_data(client_name, system_name)
        await create_table(db.bind, DynamicSimulationResultModel)

        logger.debug(f"Querying all simulation results from table '{DynamicSimulationResultModel.__tablename__}'.")
        results = db.query(DynamicSimulationResultModel).order_by(
            DynamicSimulationResultModel.TIMESTAMP.desc(),DynamicSimulationResultModel.SIMULATION_RUN_ID.desc()
        ).all()

        if not results:
            logger.warning(f"No simulation results found for client: '{client_name}', system: '{system_name}'.")
            return {
                "message": "No simulation results found",
                "client_name": client_name,
                "system_name": system_name,
                "results": []
            }
        logger.info(f"Found {len(results)} records for client: '{client_name}', system: '{system_name}'.")
        simulation_runs = {}
        for result in results:
            sim_run_id = result.SIMULATION_RUN_ID
            if sim_run_id not in simulation_runs:
                simulation_runs[sim_run_id] = {
                    "simulation_run_id": sim_run_id,
                    "timestamp": result.TIMESTAMP,
                    "fue_required": result.FUE_REQUIRED,
                    "status": result.STATUS,
                    "changes": [],
                    "summary": None
                }
            if result.STATUS and result.STATUS != simulation_runs[sim_run_id]["status"]:
                # Priority: Failed > In Progress > Completed
                current_status = simulation_runs[sim_run_id]["status"]
                new_status = result.STATUS

                if (new_status == "Failed" or
                        (new_status == "In Progress" and current_status == "Completed") or
                        (new_status == "Processing Changes" and current_status == "Completed")):
                    simulation_runs[sim_run_id]["status"] = new_status


            if result.OPERATION == "SUMMARY":
                simulation_runs[sim_run_id]["summary"] = {
                    "total_fue": result.FIELD,
                    "gb_fue": result.VALUE_LOW,
                    "gc_fue": result.VALUE_HIGH
                }
            else:
                simulation_runs[sim_run_id]["changes"].append({
                    "role": result.ROLES_CHANGED,
                    "role_description": result.ROLE_DESCRIPTION,  # Add this line
                    "object": result.OBJECT,
                    "field": result.FIELD,
                    "value_low": result.VALUE_LOW,
                    "value_high": result.VALUE_HIGH,
                    "operation": result.OPERATION,
                    "prev_license":result.PREV_LICENSE,
                    "current_license":result.CURRENT_LICENSE,
                    "status": result.STATUS
                })

        results_list = list(simulation_runs.values())
        results_list.sort(key=lambda x: x["timestamp"], reverse=True)
        logger.info(f"Organized {len(results_list)} simulation runs for client: '{client_name}', system: '{system_name}'.")
        return {
            "message": f"Found {len(results_list)} simulation runs",
            "client_name": client_name,
            "system_name": system_name,
            "results": results_list
        }

    except Exception as e:
        logger.error(f"Error retrieving simulation results: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving simulation results: {str(e)}")

