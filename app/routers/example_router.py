import uuid
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.core.logger import logger
from app.models.database import get_db
from app.models.dynamic_models import (
    create_role_obj_lic_sim_model, create_simulation_result_data,
    create_user_role_mapping_data_model
)
from datetime import datetime

from app.routers.data_loader_router import create_table

router = APIRouter(
    prefix="/simulation_result",
    tags=["Simulation Result"]
)

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
        DynamicRoleObjLicSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)
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

        result = db.execute(pivot_query).fetchone()

        if not result:
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
    try:
        # Get the highest existing simulation ID from the current table
        latest_record = db.query(DynamicSimulationResultModel.SIMULATION_RUN_ID).filter(
            DynamicSimulationResultModel.SIMULATION_RUN_ID.like('SIM%')
        ).order_by(DynamicSimulationResultModel.SIMULATION_RUN_ID.desc()).first()

        if latest_record and latest_record[0].startswith('SIM'):
            try:
                # Extract the number part and increment
                current_num = int(latest_record[0][3:])  # Remove 'SIM' prefix
                next_num = current_num + 1
            except ValueError:
                # If parsing fails, start from 100000
                next_num = 100000
        else:
            # No existing records, start from 100000
            next_num = 100000

        return f"SIM{next_num}"

    except Exception as e:
        # Fallback to default if any error occurs
        return "SIM100000"


@router.post("/run-simulation/")
async def run_simulation(
        client_name: str,
        system_name: str,
        db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Run the simulation, save results to simulation result table, and cleanup temporary tables.
    This endpoint:
    1. Runs the license classification simulation
    2. Saves the FUE results and changed roles to simulation result table
    3. Drops the temporary ROLE_OBJ_LIC_SIM table
    """
    logger.info(f"Running simulation for client: {client_name}, system: {system_name}")

    try:
        simulation_results = await get_simulation_license_classification_pivot_table(client_name, system_name, db)

        DynamicRoleObjLicSimModel = create_role_obj_lic_sim_model(client_name, system_name)
        DynamicSimulationResultModel = create_simulation_result_data(client_name, system_name)

        from app.routers.data_loader_router import create_table
        await create_table(db.bind, DynamicSimulationResultModel)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        simulation_run_id = get_next_simulation_id_for_table(db,DynamicSimulationResultModel)

        # simulation_run_id =f"SIM_REQ-{uuid.uuid4()}"
        fue_summary = simulation_results.get("fue_summary", {})
        total_fue_required = fue_summary.get("Total FUE Required", 0)


        all_role_license_data = db.query(
            DynamicRoleObjLicSimModel.AGR_NAME,
            DynamicRoleObjLicSimModel.CLASSIF_S4,
            DynamicRoleObjLicSimModel.NEW_SIM_LICE
        ).all()

        prev_licenses_by_role = {}
        current_licenses_by_role = {}

        for row in all_role_license_data:
            role = row.AGR_NAME
            prev_lic = row.CLASSIF_S4
            curr_lic = row.NEW_SIM_LICE

            if role not in prev_licenses_by_role:
                prev_licenses_by_role[role] = []
            if role not in current_licenses_by_role:
                current_licenses_by_role[role] = []

            if prev_lic:
                prev_licenses_by_role[role].append(prev_lic)
            if curr_lic:
                current_licenses_by_role[role].append(curr_lic)

        most_restrictive_prev_licenses = {
            role: get_most_restrictive_license(licenses)
            for role, licenses in prev_licenses_by_role.items()
        }

        most_restrictive_current_licenses = {
            role: get_most_restrictive_license(licenses)
            for role, licenses in current_licenses_by_role.items()
        }

        changed_roles_records = db.query(DynamicRoleObjLicSimModel).filter(
            DynamicRoleObjLicSimModel.OPERATION.isnot(None)
        ).all()

        logger.info(f"Found {len(changed_roles_records)} changed roles to save (individual changes)")

        saved_changes = 0
        for role_change_record in changed_roles_records:
            role_name = role_change_record.AGR_NAME

            derived_prev_license = most_restrictive_prev_licenses.get(role_name)
            derived_current_license = most_restrictive_current_licenses.get(role_name)

            change_record = DynamicSimulationResultModel(
                SIMULATION_RUN_ID=simulation_run_id,
                TIMESTAMP=timestamp,
                FUE_REQUIRED=str(total_fue_required),
                CLIENT_NAME=client_name,
                SYSTEM_NAME=system_name,
                ROLES_CHANGED=role_name,
                OBJECT=role_change_record.OBJECT,
                FIELD=role_change_record.FIELD,
                VALUE_LOW=role_change_record.LOW,
                VALUE_HIGH=role_change_record.HIGH,
                OPERATION=role_change_record.OPERATION,
                PREV_LICENSE=derived_prev_license,
                CURRENT_LICENSE=derived_current_license
            )
            db.add(change_record)
            saved_changes += 1

        db.commit()
        logger.info(f"Saved {saved_changes} role changes to simulation result table")

        try:
            entry_count = db.query(DynamicRoleObjLicSimModel).count()

            db.query(DynamicRoleObjLicSimModel).delete()
            db.commit()

            logger.info(
                f"Successfully deleted {entry_count} entries from simulation table: {DynamicRoleObjLicSimModel.__tablename__}")

        except Exception as delete_error:
            logger.warning(
                f"Error deleting entries from simulation table {DynamicRoleObjLicSimModel.__tablename__}: {str(delete_error)}")
            db.rollback()

        return {
            "message": "Simulation completed successfully",
            "simulation_results": simulation_results,
            "saved_changes": saved_changes,
            "fue_required": total_fue_required,
            "timestamp": timestamp,
            "simulation_run_id": simulation_run_id,
            "client_name": client_name,
            "system_name": system_name,
            "cleanup_completed": True
        }

    except Exception as e:
        db.rollback()
        logger.error(f"Error running simulation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error running simulation: {str(e)}")

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
        DynamicSimulationResultModel = create_simulation_result_data(client_name, system_name)
        await create_table(db.bind, DynamicSimulationResultModel)

        results = db.query(DynamicSimulationResultModel).order_by(
            DynamicSimulationResultModel.TIMESTAMP.desc(),DynamicSimulationResultModel.SIMULATION_RUN_ID.desc()
        ).all()

        if not results:
            return {
                "message": "No simulation results found",
                "client_name": client_name,
                "system_name": system_name,
                "results": []
            }

        simulation_runs = {}
        for result in results:
            sim_run_id = result.SIMULATION_RUN_ID
            if sim_run_id not in simulation_runs:
                simulation_runs[sim_run_id] = {
                    "simulation_run_id": sim_run_id,
                    "timestamp": result.TIMESTAMP,
                    "fue_required": result.FUE_REQUIRED,
                    "changes": [],
                    "summary": None
                }

            if result.OPERATION == "SUMMARY":
                simulation_runs[sim_run_id]["summary"] = {
                    "total_fue": result.FIELD,
                    "gb_fue": result.VALUE_LOW,
                    "gc_fue": result.VALUE_HIGH
                }
            else:
                simulation_runs[sim_run_id]["changes"].append({
                    "role": result.ROLES_CHANGED,
                    "object": result.OBJECT,
                    "field": result.FIELD,
                    "value_low": result.VALUE_LOW,
                    "value_high": result.VALUE_HIGH,
                    "operation": result.OPERATION,
                    "prev_license":result.PREV_LICENSE,
                    "current_license":result.CURRENT_LICENSE
                })

        results_list = list(simulation_runs.values())
        results_list.sort(key=lambda x: x["timestamp"], reverse=True)

        return {
            "message": f"Found {len(results_list)} simulation runs",
            "client_name": client_name,
            "system_name": system_name,
            "results": results_list
        }

    except Exception as e:
        logger.error(f"Error retrieving simulation results: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving simulation results: {str(e)}")

