
import json
import time
import uuid

from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from app.core.logger import setup_logger, get_daily_log_filename
from sqlalchemy import func, cast, Integer, inspect as sqla_inspect
from app.models.database import engine, SessionLocal
from app.models.dynamic_models import create_lice_data_model, create_auth_data_model, create_role_fiori_data_model
from app.models.request_array import RequestArray
from app.models.role_lic_re_results import LicenseOptimizationResult
from app.routers.data_loader_router import create_table
from app.schema.RequestArray import RequestArraySchema
from app.service.chatgpt import call_chatgpt_api, call_ai_api
import os
import datetime
import traceback

logger = setup_logger("app_logger")
async def get_distinct_license_types_db(db: Session, client_name: str, system_id: str) -> List[Dict[str, Any]]:
    """Retrieves distinct license types for a given client and system from the database."""
    try:
        _BaseLiceData = create_lice_data_model(client_name, system_id)
        inspector = sqla_inspect(engine)
        if not inspector.has_table(_BaseLiceData.__tablename__):
            logger.error(f"License data table not found for client '{client_name}'.")
            return []
        distinct_types = db.query(_BaseLiceData.CLASSIF_S4).distinct().all()
        return [{"id": dt[0], "name": dt[0]} for dt in distinct_types if dt[0]]
    except Exception as e:
        logger.error(f"Error fetching distinct license types for {client_name}: {e}", exc_info=True)
        return []


async def create_optimization_request_immediately(db: Session, client_name: str, system_id: str) -> str:
    """Create the request record immediately and return request ID"""
    logger.info(f"Creating optimization request for client: '{client_name}', system: '{system_id}'.")

    def get_next_request_id(db: Session) -> str:
        """Generate next sequential request ID starting from REQ100000"""
        try:
            latest_request = db.query(RequestArray).filter(
                RequestArray.req_id.like('REQ%')
            ).order_by(RequestArray.req_id.desc()).first()

            if latest_request and latest_request.req_id.startswith('REQ'):
                try:
                    current_num = int(latest_request.req_id[3:])
                    next_num = current_num + 1
                    logger.debug(f"Generated next request ID from latest: {latest_request.req_id}")

                except ValueError:
                    next_num = 100000
                    logger.warning("Could not parse latest request ID number. Starting from default 'REQ100000'.")

            else:
                next_num = 100000
                logger.info("No previous requests found. Starting from default 'REQ100000'.")
            return f"REQ{next_num}"
        except Exception as e:
            logger.error(f"Error generating request ID, defaulting to REQ100000: {e}", exc_info=True)
            return "REQ100000"

    # Ensure tables exist
    await create_table(engine, RequestArray)
    await create_table(engine, LicenseOptimizationResult)
    logger.debug("Request and result tables verified.")

    # Create request record
    request_id = get_next_request_id(db)
    new_request = RequestArray(
        req_id=request_id,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_id,
        STATUS="IN_PROGRESS"
    )

    db.add(new_request)
    db.commit()
    db.refresh(new_request)

    logger.info(f"Request record created: {request_id}")

    return request_id


def process_optimization_in_background(
        client_name: str,
        system_id: str,
        request_id: str,
        ratio_threshold: Optional[int],
        target_license: str,
        sap_system_info: str,
        role_names: Optional[List[str]]
):
    """Process the optimization in the background using a new database session"""
    # Create a new database session for the background task
    db = SessionLocal()

    try:
        logger.info(f"Starting background optimization for request: {request_id}")

        # Get the request record
        request_record = db.query(RequestArray).filter(RequestArray.req_id == request_id).first()

        if not request_record:
            logger.error(f"Request record not found for ID: {request_id}")
            return

        # Run the optimization logic (modified version without request creation part)
        result = run_optimization_processing(
            db, client_name, system_id, request_id, ratio_threshold,
            target_license, sap_system_info, role_names
        )

        # Update status based on result
        if isinstance(result, dict) and "error" in result:
            request_record.STATUS = "FAILED"
            logger.error(f"Background optimization failed for request {request_id}: {result['error']}")
        else:
            request_record.STATUS = "COMPLETED"
            logger.info(f"Background optimization completed successfully for request: {request_id}")

        db.commit()

    except Exception as e:
        logger.error(f"Background optimization failed for request {request_id}: {str(e)}", exc_info=True)

        # Update status to failed
        try:
            request_record = db.query(RequestArray).filter(RequestArray.req_id == request_id).first()
            if request_record:
                request_record.STATUS = "FAILED"
                db.commit()
        except Exception as update_error:
            logger.error(f"Failed to update request status to FAILED: {update_error}", exc_info=True)

    finally:
        db.close()


def run_optimization_processing(
        db: Session,
        client_name: str,
        system_id: str,
        request_id: str,
        ratio_threshold: Optional[int],
        target_license: str,
        sap_system_info: str,
        role_names: Optional[List[str]]
) -> Dict[str, Any]:
    """
    Modified version of your optimize_license_logic without the request creation part
    """
    logger.info(f"Starting optimization processing for request: {request_id}")

    # Initialize variables
    _BaseLiceData, _BaseAuthData, _BaseFioriData = None, None, None
    results = {}

    try:
        # Your existing model creation logic
        _BaseLiceData = create_lice_data_model(client_name, system_id)
        _BaseAuthData = create_auth_data_model(client_name, system_id)
        _BaseFioriData = create_role_fiori_data_model(client_name, system_id)

        inspector = sqla_inspect(engine)
        if not inspector.has_table(_BaseLiceData.__tablename__):
            logger.error(f"License data table not found for client '{client_name}'. Load data first.")
            return {"error": f"License data table not found for client '{client_name}'. Load data first.",
                    "status_code": 404}
        if not inspector.has_table(_BaseAuthData.__tablename__):
            logger.error(f"Authorization data table not found for client '{client_name}'. Load data first.")
            return {"error": f"Authorization data table not found for client '{client_name}'. Load data first.",
                    "status_code": 404}

        fiori_table_exists = inspector.has_table(_BaseFioriData.__tablename__)
        if not fiori_table_exists:
            logger.warning(f"Fiori data table not found for client '{client_name}'. Continuing without Fiori data.")

        # Your existing query logic
        query = db.query(_BaseLiceData).filter(
            _BaseLiceData.CLASSIF_S4 == target_license
        )

        if ratio_threshold is not None:
            try:
                query = query.filter(
                    _BaseLiceData.AGR_RATIO.isnot(None),
                    _BaseLiceData.AGR_RATIO != '',
                    cast(func.split_part(_BaseLiceData.AGR_RATIO, '/', 1), Integer) <= ratio_threshold
                )
            except Exception as ratio_e:
                logger.warning(f"Could not apply ratio filter due to data format issue or DB error: {ratio_e}",
                               exc_info=True)

        if role_names:
            logger.debug(f"Filtering by specific roles: {role_names}")
            query = query.filter(_BaseLiceData.AGR_NAME.in_(role_names))

        roles_data = query.all()
        if not roles_data:
            msg = f"No roles found matching criteria for client '{client_name}'"
            if role_names:
                msg += f" and specific roles {role_names}"
            logger.info(msg)
            return {"message": msg, "status_code": 404}

        distinct_roles = sorted(list({r.AGR_NAME for r in roles_data}))

        # Your existing role processing logic
        all_roles_json = []
        role_descriptions = {}

        for role in distinct_roles:
            logger.debug(f"Processing role: {role}")
            role_records = [r for r in roles_data if r.AGR_NAME == role]
            if not role_records:
                logger.info(f"No data found in BaseLiceData for role: {role}. Skipping both lice and auth checks.")
                continue

            role_description = "No Role Description found"
            for record in role_records:
                if record.AGR_NAME == role and hasattr(record, 'AGR_TEXT') and record.AGR_TEXT:
                    role_description = record.AGR_TEXT
                    break

            role_descriptions[role] = role_description

            authorization_objects = [
                {"object": rec.OBJECT, "field": rec.FIELD, "value": rec.LOW, "description": rec.TTEXT}
                for rec in role_records
            ]

            tcodes_query = db.query(_BaseAuthData.AUTH_VALUE_LOW).filter(
                _BaseAuthData.AGR_NAME == role,
                _BaseAuthData.OBJECT == "S_TCODE",
                _BaseAuthData.FIELD_NAME == "TCD"
            ).distinct()
            tcodes = tcodes_query.all()

            transaction_codes = sorted([t.AUTH_VALUE_LOW for t in tcodes])

            fiori_apps = []
            if fiori_table_exists:
                try:
                    fiori_query = db.query(_BaseFioriData).filter(
                        _BaseFioriData.ROLE == role
                    ).all()

                    fiori_apps_dict = {}
                    for fiori_record in fiori_query:
                        app_name = fiori_record.TITLE_SUBTITLE_INFORMATION
                        action = fiori_record.ACTION

                        if app_name and app_name.strip():
                            if app_name not in fiori_apps_dict:
                                fiori_apps_dict[app_name] = {"app": app_name, "actions": []}

                            if action and action.strip() and action not in fiori_apps_dict[app_name]["actions"]:
                                fiori_apps_dict[app_name]["actions"].append(action)

                    fiori_apps = list(fiori_apps_dict.values())
                    logger.debug(f"Found {len(fiori_apps)} Fiori apps for role '{role}'")

                except Exception as fiori_e:
                    logger.warning(f"Error fetching Fiori data for role '{role}': {fiori_e}", exc_info=True)
                    fiori_apps = []

            role_json = {
                "role": role,
                "currentLicense": target_license,
                "authorizationObjects": authorization_objects,
                "transactionCodes": transaction_codes,
                "fioriApps": fiori_apps
            }

            all_roles_json.append(role_json)

        if all_roles_json:
            # Your dummy response logic
            dummy_ai_response_obj = {
                "Z:WM:FDBD:EWM:ALL:DISPLAY": [
                    {
                        "authorizationObject": "C_APO_PROD",
                        "field": "ACTVT",
                        "value": "16",
                        "licenseCanBeReduced": "Yes",
                        "insights": "APO product master authorization not needed for EWM display role",
                        "recommendation": "Remove C_APO_PROD object entirely from this role",
                        "explanation": "The role Z:WM:FDBD:EWM:ALL:DISPLAY contains EWM and core logistics transactions but no APO-specific transactions. C_APO_PROD is an APO authorization object for product master data that is not required for any of the listed EWM transactions like /SCWM/MON, /SCWM/RFUI, or standard SAP transactions like MIGO, MB51. This authorization elevates the license requirement unnecessarily."
                    }
                ],
                "ZD_DTS_M_QM_T_DS_1300": [
                    {
                        "authorizationObject": "Q_INSPPNT",
                        "field": "ACTVT",
                        "value": "01",
                        "licenseCanBeReduced": "No",
                        "insights": "Create authorization required for QE01 transaction functionality",
                        "recommendation": "Keep as-is for quality inspection point creation",
                        "explanation": "The role ZD_DTS_M_QM_T_DS_1300 includes transaction QE01 which is used to create inspection results. This requires Q_INSPPNT with ACTVT 01 (Create) authorization. The authorization directly supports the quality management transactions in this role and is appropriately scoped."
                    }
                ]
            }
            time.sleep(10)  # Your 10-second processing delay

            ai_response = json.dumps(dummy_ai_response_obj)
            logger.debug(f"Dummy AI response for all roles: {ai_response}")

            try:
                authorization_analysis = json.loads(ai_response)

                for role_id, auth_objects in authorization_analysis.items():
                    if not isinstance(auth_objects, list):
                        continue
                    for item in auth_objects:
                        role_description = role_descriptions.get(role_id, "No Role Description found")
                        db_result = LicenseOptimizationResult(
                            REQ_ID=request_id,  # Use the passed request_id
                            ROLE_ID=role_id,
                            ROLE_DESCRIPTION=role_description,
                            AUTHORIZATION_OBJECT=item.get("authorizationObject"),
                            FIELD=item.get("field"),
                            VALUE=item.get("value"),
                            LICENSE_REDUCIBLE=item.get("licenseCanBeReduced"),
                            INSIGHTS=item.get("insights"),
                            RECOMMENDATIONS=item.get("recommendation"),
                            EXPLANATIONS=item.get("explanation")
                        )
                        db.add(db_result)

                db.commit()

            except Exception as e:
                logger.error(f"Failed to parse AI response for combined roles: {str(e)}", exc_info=True)
                for role in distinct_roles:
                    results[role] = [{
                        "error": f"Failed to parse AI response for combined roles: {str(e)}"
                    }]

        # File writing logic
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"{client_name}-{system_id}-{timestamp}.json"
        filepath = f"output/{filename}"
        os.makedirs("output", exist_ok=True)

        try:
            with open(filepath, 'w') as f:
                json.dump(results, f, indent=4)
            logger.info(f"Results written to: {filepath}")
        except Exception as e:
            logger.error(f"Error writing results to file: {e}", exc_info=True)
            return {"error": f"Error writing results to file: {e}", "status_code": 500}

        return results

    except Exception as e:
        logger.error(f"Unexpected error during optimization processing: {e}", exc_info=True)
        return {"error": f"An unexpected server error occurred during optimization.", "details": str(e),
                "status_code": 500}


#the one with dummy response
# async def optimize_license_logic(
#         db: Session,
#         client_name: str,
#         system_id: str,
#         ratio_threshold: Optional[int] = None,
#         target_license: str = "GB Advanced Use",
#         sap_system_info: str = "S4 HANA OnPremise 2021 Support Pack 01, Basis Release 751",
#         role_names: Optional[List[str]] = None
# ) -> Dict[str, Any]:
#     """Core logic for license optimization analysis."""
#     logger.info(f"Starting license optimization for client: {client_name}, roles: {role_names}")
#
#     # Initialize all variables at function level to ensure proper scope
#     _BaseLiceData, _BaseAuthData, _BaseFioriData = None, None, None
#     new_request = None
#     req_id = None
#     results = {}
#
#     try:
#         _BaseLiceData = create_lice_data_model(client_name, system_id)
#         _BaseAuthData = create_auth_data_model(client_name, system_id)
#         _BaseFioriData = create_role_fiori_data_model(client_name, system_id)
#
#         inspector = sqla_inspect(engine)
#         if not inspector.has_table(_BaseLiceData.__tablename__):
#             logger.error(f"License data table not found for client '{client_name}'. Load data first.")
#             return {"error": f"License data table not found for client '{client_name}'. Load data first.",
#                     "status_code": 404}
#         if not inspector.has_table(_BaseAuthData.__tablename__):
#             logger.error(f"Authorization data table not found for client '{client_name}'. Load data first.")
#             return {"error": f"Authorization data table not found for client '{client_name}'. Load data first.",
#                     "status_code": 404}
#
#         fiori_table_exists = inspector.has_table(_BaseFioriData.__tablename__)
#         if not fiori_table_exists:
#             logger.warning(f"Fiori data table not found for client '{client_name}'. Continuing without Fiori data.")
#
#     except Exception as e:
#         print(f"Error getting dynamic models for client '{client_name}': {e}")
#         logger.error(f"Error getting dynamic models for client '{client_name}': {e}", exc_info=True)
#         return {"error": f"Invalid client name or configuration error for '{client_name}'", "details": str(e),
#                 "status_code": 400}
#
#     await create_table(engine, RequestArray)
#     await create_table(engine, LicenseOptimizationResult)
#
#     def get_next_request_id(db: Session) -> str:
#         """Generate next sequential request ID starting from REQ100000"""
#         try:
#             latest_request = db.query(RequestArray).filter(
#                 RequestArray.req_id.like('REQ%')
#             ).order_by(RequestArray.req_id.desc()).first()
#
#             if latest_request and latest_request.req_id.startswith('REQ'):
#                 try:
#                     current_num = int(latest_request.req_id[3:])
#                     next_num = current_num + 1
#                 except ValueError:
#                     next_num = 100000
#             else:
#                 next_num = 100000
#             return f"REQ{next_num}"
#         except Exception as e:
#             return "REQ100000"
#
#     # Create request record
#     RequestArray1 = RequestArray
#     LicenseOptimizationResult1 = LicenseOptimizationResult
#
#     try:
#         request_id = get_next_request_id(db)
#         new_request = RequestArray1(
#             req_id=request_id,
#             CLIENT_NAME=client_name,
#             SYSTEM_NAME=system_id,
#             STATUS="IN_PROGRESS"
#         )
#         db.add(new_request)
#         db.commit()
#         db.refresh(new_request)
#         req_id = new_request.req_id  # This is crucial - set req_id here
#
#         # Main processing logic
#         query = db.query(_BaseLiceData).filter(
#             _BaseLiceData.CLASSIF_S4 == target_license
#         )
#
#         if ratio_threshold is not None:
#             try:
#                 query = query.filter(
#                     _BaseLiceData.AGR_RATIO.isnot(None),
#                     _BaseLiceData.AGR_RATIO != '',
#                     cast(func.split_part(_BaseLiceData.AGR_RATIO, '/', 1), Integer) <= ratio_threshold
#                 )
#             except Exception as ratio_e:
#                 logger.warning(f"Could not apply ratio filter due to data format issue or DB error: {ratio_e}",
#                                exc_info=True)
#                 print(f"Warning: Could not apply ratio filter due to data format issue or DB error: {ratio_e}")
#
#         if role_names:
#             logger.debug(f"Filtering by specific roles: {role_names}")
#             query = query.filter(_BaseLiceData.AGR_NAME.in_(role_names))
#
#         roles_data = query.all()
#         if not roles_data:
#             msg = f"No roles found matching criteria for client '{client_name}'"
#             if role_names:
#                 msg += f" and specific roles {role_names}"
#             logger.info(msg)
#             return {"message": msg, "status_code": 404}
#
#         distinct_roles = sorted(list({r.AGR_NAME for r in roles_data}))
#
#         # Collect all roles data first, then send in single request
#         all_roles_json = []
#         role_descriptions = {}
#
#         for role in distinct_roles:
#             logger.debug(f"Processing role: {role}")
#             role_records = [r for r in roles_data if r.AGR_NAME == role]
#             if not role_records:
#                 logger.info(f"No data found in BaseLiceData for role: {role}. Skipping both lice and auth checks.")
#                 continue
#
#             role_description = "No Role Description found"
#             for record in role_records:
#                 if record.AGR_NAME == role and hasattr(record, 'AGR_TEXT') and record.AGR_TEXT:
#                     role_description = record.AGR_TEXT
#                     break
#
#             role_descriptions[role] = role_description
#
#             authorization_objects = [
#                 {"object": rec.OBJECT, "field": rec.FIELD, "value": rec.LOW, "description": rec.TTEXT}
#                 for rec in role_records
#             ]
#
#             tcodes_query = db.query(_BaseAuthData.AUTH_VALUE_LOW).filter(
#                 _BaseAuthData.AGR_NAME == role,
#                 _BaseAuthData.OBJECT == "S_TCODE",
#                 _BaseAuthData.FIELD_NAME == "TCD"
#             ).distinct()
#             tcodes = tcodes_query.all()
#
#             transaction_codes = sorted([t.AUTH_VALUE_LOW for t in tcodes])
#
#             fiori_apps = []
#             if fiori_table_exists:
#                 try:
#                     fiori_query = db.query(_BaseFioriData).filter(
#                         _BaseFioriData.ROLE == role
#                     ).all()
#
#                     fiori_apps_dict = {}
#                     for fiori_record in fiori_query:
#                         app_name = fiori_record.TITLE_SUBTITLE_INFORMATION
#                         action = fiori_record.ACTION
#
#                         if app_name and app_name.strip():
#                             if app_name not in fiori_apps_dict:
#                                 fiori_apps_dict[app_name] = {"app": app_name, "actions": []}
#
#                             if action and action.strip() and action not in fiori_apps_dict[app_name]["actions"]:
#                                 fiori_apps_dict[app_name]["actions"].append(action)
#
#                     fiori_apps = list(fiori_apps_dict.values())
#                     logger.debug(f"Found {len(fiori_apps)} Fiori apps for role '{role}'")
#
#                 except Exception as fiori_e:
#                     logger.warning(f"Error fetching Fiori data for role '{role}': {fiori_e}", exc_info=True)
#                     fiori_apps = []
#
#             role_json = {
#                 "role": role,
#                 "currentLicense": target_license,
#                 "authorizationObjects": authorization_objects,
#                 "transactionCodes": transaction_codes,
#                 "fioriApps": fiori_apps
#             }
#
#             all_roles_json.append(role_json)
#
#         if all_roles_json:
#             # Comment out the AI API call and use a dummy response
#             # prompt = f"""..."""
#             # try:
#             #     logger.info(f"prompt: {prompt}")
#             #     ai_response = call_ai_api(prompt)
#             #     ...
#
#             # --- DUMMY RESPONSE FOR TESTING ---
#             dummy_ai_response_obj = {
#                 "Z:WM:FDBD:EWM:ALL:DISPLAY": [
#                     {
#                         "authorizationObject": "C_APO_PROD",
#                         "field": "ACTVT",
#                         "value": "16",
#                         "licenseCanBeReduced": "Yes",
#                         "insights": "APO product master authorization not needed for EWM display role",
#                         "recommendation": "Remove C_APO_PROD object entirely from this role",
#                         "explanation": "The role Z:WM:FDBD:EWM:ALL:DISPLAY contains EWM and core logistics transactions but no APO-specific transactions. C_APO_PROD is an APO authorization object for product master data that is not required for any of the listed EWM transactions like /SCWM/MON, /SCWM/RFUI, or standard SAP transactions like MIGO, MB51. This authorization elevates the license requirement unnecessarily."
#                     }
#                 ],
#                 "ZD_DTS_M_QM_T_DS_1300": [
#                     {
#                         "authorizationObject": "Q_INSPPNT",
#                         "field": "ACTVT",
#                         "value": "01",
#                         "licenseCanBeReduced": "No",
#                         "insights": "Create authorization required for QE01 transaction functionality",
#                         "recommendation": "Keep as-is for quality inspection point creation",
#                         "explanation": "The role ZD_DTS_M_QM_T_DS_1300 includes transaction QE01 which is used to create inspection results. This requires Q_INSPPNT with ACTVT 01 (Create) authorization. The authorization directly supports the quality management transactions in this role and is appropriately scoped."
#                     },
#                     {
#                         "authorizationObject": "C_APO_LOC",
#                         "field": "ACTVT",
#                         "value": "01",
#                         "licenseCanBeReduced": "Yes",
#                         "insights": "APO location create authorization not needed for QM role",
#                         "recommendation": "Remove C_APO_LOC with ACTVT 01 from this role",
#                         "explanation": "The role ZD_DTS_M_QM_T_DS_1300 is focused on quality management transactions (QA02, QE01, QE02, etc.) and does not contain any APO-specific transactions. C_APO_LOC with ACTVT 01 (Create) is an APO authorization for location master data that is not required for the QM and EWM monitoring transactions listed."
#                     },
#                     {
#                         "authorizationObject": "C_APO_LOC",
#                         "field": "ACTVT",
#                         "value": "02",
#                         "licenseCanBeReduced": "Yes",
#                         "insights": "APO location change authorization not needed for QM role",
#                         "recommendation": "Remove C_APO_LOC with ACTVT 02 from this role",
#                         "explanation": "Similar to the create authorization, the change authorization (ACTVT 02) for APO locations is not required for this quality management focused role. None of the transactions listed require APO location maintenance capabilities. Removing this will help reduce the FUE license consumption."
#                     },
#                     {
#                         "authorizationObject": "Q_CTRLCHRT",
#                         "field": "ACTVT",
#                         "value": "B2",
#                         "licenseCanBeReduced": "May Be",
#                         "insights": "Control chart authorization may be needed for QGA3 transaction",
#                         "recommendation": "Verify if QGA3 usage requires this, otherwise remove",
#                         "explanation": "Q_CTRLCHRT with ACTVT B2 is for quality control charts. Transaction QGA3 (Display Control Chart) is included in the role. If users only need display access, this authorization with B2 activity might be excessive. Consider changing to display-only authorization (ACTVT 03) if create/change functionality is not required, which could potentially lower the license requirement."
#                     }
#                 ]
#             }
#             time.sleep(10)
#
#             ai_response = json.dumps(dummy_ai_response_obj)
#             print(ai_response)
#             logger.debug(f"Dummy AI response for all roles: {ai_response}")
#             # --- END DUMMY RESPONSE ---
#
#             try:
#                 # The rest of the parsing and database logic remains the same
#                 authorization_analysis = json.loads(ai_response)
#
#                 for role_id, auth_objects in authorization_analysis.items():
#                     if not isinstance(auth_objects, list):
#                         continue  # Skip if not a list
#                     for item in auth_objects:
#                         # Assuming the item has the necessary keys
#                         role_description = role_descriptions.get(role_id, "No Role Description found")
#                         db_result = LicenseOptimizationResult1(
#                             REQ_ID=req_id,
#                             ROLE_ID=role_id,
#                             ROLE_DESCRIPTION=role_description,
#                             AUTHORIZATION_OBJECT=item.get("authorizationObject"),
#                             FIELD=item.get("field"),
#                             VALUE=item.get("value"),
#                             LICENSE_REDUCIBLE=item.get("licenseCanBeReduced"),
#                             INSIGHTS=item.get("insights"),
#                             RECOMMENDATIONS=item.get("recommendation"),
#                             EXPLANATIONS=item.get("explanation")
#                         )
#                         db.add(db_result)
#
#                 db.commit()
#
#             except Exception as e:
#                 logger.error(f"Failed to parse AI response for combined roles: {str(e)}", exc_info=True)
#                 # Create error response for all roles
#                 for role in distinct_roles:
#                     results[role] = [{
#                         "error": f"Failed to parse AI response for combined roles: {str(e)}"
#                     }]
#
#         # File writing
#         timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
#         filename = f"{client_name}-{system_id}-{timestamp}.json"
#         filepath = f"output/{filename}"
#         os.makedirs("output", exist_ok=True)
#
#         try:
#             with open(filepath, 'w') as f:
#                 json.dump(results, f, indent=4)
#             logger.info(f"Results written to: {filepath}")
#         except Exception as e:
#             logger.error(f"Error writing results to file: {e}", exc_info=True)
#             if new_request:
#                 new_request.STATUS = "FAILED"
#                 db.commit()
#             return {"error": f"Error writing results to file: {e}", "status_code": 500}
#
#         # Mark as completed
#         if new_request:
#             new_request.STATUS = "COMPLETED"
#             db.commit()
#
#         return results
#
#     except Exception as e:
#         print(
#             f"Unexpected error during license optimization query/processing for client {client_name} and system id {system_id}: {e}")
#         logger.error(
#             f"Unexpected error during license optimization query/processing for client {client_name} and system id {system_id}: {e}",
#             exc_info=True)
#         traceback.print_exc()
#
#         if new_request:
#             new_request.STATUS = "FAILED"
#             db.commit()
#
#         return {"error": f"An unexpected server error occurred during optimization.", "details": str(e),
#                 "status_code": 500}

async def get_all_requests_service(db: Session) -> List[RequestArraySchema]:
    """Service function to get all requests with exception handling"""
    try:
        requests = db.query(RequestArray).order_by(RequestArray.TIMESTAMP.desc()).all()
        return requests
    except Exception as e:
        logger.error(f"Failed to fetch requests: {str(e)}", exc_info=True)
        raise



# normal from github
# async def optimize_license_logic(
#             db: Session,
#             client_name: str,
#             system_id: str,
#             ratio_threshold: Optional[int] = None,
#             target_license: str = "GB Advanced Use",
#             sap_system_info: str = "S4 HANA OnPremise 2021 Support Pack 01, Basis Release 751",
#             role_names: Optional[List[str]] = None
#     ) -> Dict[str, Any]:
#         """Core logic for license optimization analysis."""
#         logger.info(f"Starting license optimization for client: {client_name}, roles: {role_names}")
#
#         # Initialize all variables at function level to ensure proper scope
#         _BaseLiceData, _BaseAuthData = None, None
#         new_request = None
#         req_id = None
#         results = {}
#
#         try:
#             _BaseLiceData = create_lice_data_model(client_name, system_id)
#             _BaseAuthData = create_auth_data_model(client_name, system_id)
#             _BaseFioriData = create_role_fiori_data_model(client_name, system_id)
#
#             inspector = sqla_inspect(engine)
#             if not inspector.has_table(_BaseLiceData.__tablename__):
#                 logger.error(f"License data table not found for client '{client_name}'. Load data first.")
#                 return {"error": f"License data table not found for client '{client_name}'. Load data first.",
#                         "status_code": 404}
#             if not inspector.has_table(_BaseAuthData.__tablename__):
#                 logger.error(f"Authorization data table not found for client '{client_name}'. Load data first.")
#                 return {"error": f"Authorization data table not found for client '{client_name}'. Load data first.",
#                         "status_code": 404}
#
#             fiori_table_exists = inspector.has_table(_BaseFioriData.__tablename__)
#             if not fiori_table_exists:
#                 logger.warning(f"Fiori data table not found for client '{client_name}'. Continuing without Fiori data.")
#
#         except Exception as e:
#             print(f"Error getting dynamic models for client '{client_name}': {e}")
#             logger.error(f"Error getting dynamic models for client '{client_name}': {e}", exc_info=True)
#             return {"error": f"Invalid client name or configuration error for '{client_name}'", "details": str(e),
#                     "status_code": 400}
#
#         await create_table(engine, RequestArray)
#         await create_table(engine, LicenseOptimizationResult)
#
#         def get_next_request_id(db: Session) -> str:
#             """Generate next sequential request ID starting from REQ100000"""
#             try:
#                 latest_request = db.query(RequestArray).filter(
#                     RequestArray.req_id.like('REQ%')
#                 ).order_by(RequestArray.req_id.desc()).first()
#
#                 if latest_request and latest_request.req_id.startswith('REQ'):
#                     try:
#                         current_num = int(latest_request.req_id[3:])
#                         next_num = current_num + 1
#                     except ValueError:
#                         next_num = 100000
#                 else:
#                     next_num = 100000
#                 return f"REQ{next_num}"
#             except Exception as e:
#                 return "REQ100000"
#
#         # Create request record
#         RequestArray1 = RequestArray
#         LicenseOptimizationResult1 = LicenseOptimizationResult
#
#         try:
#             request_id = get_next_request_id(db)
#             new_request = RequestArray1(
#                 req_id=request_id,
#                 CLIENT_NAME=client_name,
#                 SYSTEM_NAME=system_id,
#                 STATUS="IN_PROGRESS"
#             )
#             db.add(new_request)
#             db.commit()
#             db.refresh(new_request)
#             req_id = new_request.req_id  # This is crucial - set req_id here
#
#             # Main processing logic
#             query = db.query(_BaseLiceData).filter(
#                 _BaseLiceData.CLASSIF_S4 == target_license
#             )
#
#             if ratio_threshold is not None:
#                 try:
#                     query = query.filter(
#                         _BaseLiceData.AGR_RATIO.isnot(None),
#                         _BaseLiceData.AGR_RATIO != '',
#                         cast(func.split_part(_BaseLiceData.AGR_RATIO, '/', 1), Integer) <= ratio_threshold
#                     )
#                 except Exception as ratio_e:
#                     logger.warning(f"Could not apply ratio filter due to data format issue or DB error: {ratio_e}",
#                                    exc_info=True)
#                     print(f"Warning: Could not apply ratio filter due to data format issue or DB error: {ratio_e}")
#
#             if role_names:
#                 logger.debug(f"Filtering by specific roles: {role_names}")
#                 query = query.filter(_BaseLiceData.AGR_NAME.in_(role_names))
#
#             roles_data = query.all()
#             if not roles_data:
#                 msg = f"No roles found matching criteria for client '{client_name}'"
#                 if role_names:
#                     msg += f" and specific roles {role_names}"
#                 logger.info(msg)
#                 return {"message": msg, "status_code": 404}
#
#             distinct_roles = sorted(list({r.AGR_NAME for r in roles_data}))
#
#             # Collect all roles data first, then send in single request
#             all_roles_json = []
#             role_descriptions = {}
#
#             for role in distinct_roles:
#                 logger.debug(f"Processing role: {role}")
#                 role_records = [r for r in roles_data if r.AGR_NAME == role]
#                 if not role_records:
#                     logger.info(f"No data found in BaseLiceData for role: {role}. Skipping both lice and auth checks.")
#                     continue
#
#                 role_description = "No Role Description found"
#                 for record in role_records:
#                     if record.AGR_NAME == role and hasattr(record, 'AGR_TEXT') and record.AGR_TEXT:
#                         role_description = record.AGR_TEXT
#                         break
#
#                 role_descriptions[role] = role_description
#
#                 authorization_objects = [
#                     {"object": rec.OBJECT, "field": rec.FIELD, "value": rec.LOW, "description": rec.TTEXT}
#                     for rec in role_records
#                 ]
#
#                 tcodes_query = db.query(_BaseAuthData.AUTH_VALUE_LOW).filter(
#                     _BaseAuthData.AGR_NAME == role,
#                     _BaseAuthData.OBJECT == "S_TCODE",
#                     _BaseAuthData.FIELD_NAME == "TCD"
#                 ).distinct()
#                 tcodes = tcodes_query.all()
#
#                 transaction_codes = sorted([t.AUTH_VALUE_LOW for t in tcodes])
#
#                 fiori_apps = []
#                 if fiori_table_exists:
#                     try:
#                         fiori_query = db.query(_BaseFioriData).filter(
#                             _BaseFioriData.ROLE == role
#                         ).all()
#
#                         fiori_apps_dict = {}
#                         for fiori_record in fiori_query:
#                             app_name = fiori_record.TITLE_SUBTITLE_INFORMATION
#                             action = fiori_record.ACTION
#
#                             if app_name and app_name.strip():
#                                 if app_name not in fiori_apps_dict:
#                                     fiori_apps_dict[app_name] = {"app": app_name, "actions": []}
#
#                                 if action and action.strip() and action not in fiori_apps_dict[app_name]["actions"]:
#                                     fiori_apps_dict[app_name]["actions"].append(action)
#
#                         fiori_apps = list(fiori_apps_dict.values())
#                         logger.debug(f"Found {len(fiori_apps)} Fiori apps for role '{role}'")
#
#                     except Exception as fiori_e:
#                         logger.warning(f"Error fetching Fiori data for role '{role}': {fiori_e}", exc_info=True)
#                         fiori_apps = []
#
#                 role_json = {
#                     "role": role,
#                     "currentLicense": target_license,
#                     "authorizationObjects": authorization_objects,
#                     "transactionCodes": transaction_codes,
#                     "fioriApps": fiori_apps
#                 }
#
#                 all_roles_json.append(role_json)
#
#             if all_roles_json:
#                 prompt = f"""I'm optimizing SAP FUE license consumption for an SAP role.
#     Client: {client_name}
#     SAP System ID:{system_id}
#     SAP System Info: {sap_system_info}
#     Here's the role data in JSON:
#     {json.dumps(all_roles_json, indent=2)}
#     Task: Check if these authorization objects are required for the role's intended functions, based on the listed transaction codes. Suggest changes to reduce FUE license consumption (e.g., to GC Core Use or GC Self Service Use) by adjusting or removing objects.
#     Please provide the response in this exact JSON format for each authorization object.Ensure that data is provided for every field in the JSON structure below.:
#     [
#       {{"authorizationObject": "string", "field": "string", "value": "string", "licenseCanBeReduced": "Yes/No/May Be", "insights": "Short reason why reduction is possible or not", "recommendation": "Short fix to lower license consumption", "explanation": "Detailed explanation of the analysis"}}
#     ]
#     Output ONLY the valid JSON array. No additional text or markdown formatting.
#     """
#
#                 try:
#                     logger.info(f"prompt: {prompt}")
#                     ai_response = call_ai_api(prompt)
#                     print(ai_response)
#                     logger.debug(f"AI API response for all roles: {ai_response}")
#
#                     if ai_response.startswith("```json") or ai_response.startswith("```"):
#                         ai_response = ai_response.strip("```json").strip("```").strip()
#
#                     authorization_analysis = json.loads(ai_response)
#
#                     for item in authorization_analysis:
#                         auth_object = item.get("authorizationObject")
#
#                         matched_role = None
#                         for role_data in all_roles_json:
#                             role_name = role_data["role"]
#                             for auth_obj in role_data["authorizationObjects"]:
#                                 if auth_obj["object"] == auth_object:
#                                     matched_role = role_name
#                                     break
#                             if matched_role:
#                                 break
#
#                         if matched_role:
#                             if matched_role not in results:
#                                 results[matched_role] = []
#                             results[matched_role].append(item)
#
#                             # Save to database - req_id is guaranteed to be set at this point
#                             role_description = role_descriptions.get(matched_role, "No Role Description found")
#                             db_result = LicenseOptimizationResult1(
#                                 REQ_ID=req_id,  # This should now work since req_id is set above
#                                 ROLE_ID=matched_role,
#                                 ROLE_DESCRIPTION=role_description,
#                                 AUTHORIZATION_OBJECT=item.get("authorizationObject"),
#                                 FIELD=item.get("field"),
#                                 VALUE=item.get("value"),
#                                 LICENSE_REDUCIBLE=item.get("licenseCanBeReduced"),
#                                 INSIGHTS=item.get("insights"),
#                                 RECOMMENDATIONS=item.get("recommendation"),
#                                 EXPLANATIONS=item.get("explanation")
#                             )
#                             db.add(db_result)
#
#                     db.commit()
#
#                 except Exception as e:
#                     logger.error(f"Failed to parse AI response for combined roles: {str(e)}", exc_info=True)
#                     # Create error response for all roles
#                     for role in distinct_roles:
#                         results[role] = [{
#                             "error": f"Failed to parse AI response for combined roles: {str(e)}"
#                         }]
#
#             # File writing
#             timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
#             filename = f"{client_name}-{system_id}-{timestamp}.json"
#             filepath = f"output/{filename}"
#             os.makedirs("output", exist_ok=True)
#
#             try:
#                 with open(filepath, 'w') as f:
#                     json.dump(results, f, indent=4)
#                 logger.info(f"Results written to: {filepath}")
#             except Exception as e:
#                 logger.error(f"Error writing results to file: {e}", exc_info=True)
#                 if new_request:
#                     new_request.STATUS = "FAILED"
#                     db.commit()
#                 return {"error": f"Error writing results to file: {e}", "status_code": 500}
#
#             # Mark as completed
#             if new_request:
#                 new_request.STATUS = "COMPLETED"
#                 db.commit()
#
#             return results
#
#         except Exception as e:
#             print(
#                 f"Unexpected error during license optimization query/processing for client {client_name} and system id {system_id}: {e}")
#             logger.error(
#                 f"Unexpected error during license optimization query/processing for client {client_name} and system id {system_id}: {e}",
#                 exc_info=True)
#             traceback.print_exc()
#
#             if new_request:
#                 new_request.STATUS = "FAILED"
#                 db.commit()
#
#             return {"error": f"An unexpected server error occurred during optimization.", "details": str(e),
#                     "status_code": 500}
