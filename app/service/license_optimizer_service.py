import json
import uuid

from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from app.core.logger import logger
from sqlalchemy import func, cast, Integer, inspect as sqla_inspect
from app.models.database import engine
from app.models.dynamic_models import create_lice_data_model, create_auth_data_model, create_role_fiori_data_model
from app.models.request_array import RequestArray
from app.models.role_lic_re_results import LicenseOptimizationResult
from app.routers.data_loader_router import create_table
from app.service.chatgpt import call_chatgpt_api, call_ai_api
import os
import datetime
import traceback

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

async def optimize_license_logic(
        db: Session,
        client_name: str,
        system_id: str,
        ratio_threshold: Optional[int] = None,
        target_license: str = "GB Advanced Use",
        sap_system_info: str = "S4 HANA OnPremise 2021 Support Pack 01, Basis Release 751",
        role_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Core logic for license optimization analysis."""
    logger.info(f"Starting license optimization for client: {client_name}, roles: {role_names}")

    _BaseLiceData, _BaseAuthData = None, None
    new_request = None

    try:
        _BaseLiceData = create_lice_data_model(client_name, system_id)
        _BaseAuthData = create_auth_data_model(client_name, system_id)
        # newly added
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
        # newly added
        fiori_table_exists = inspector.has_table(_BaseFioriData.__tablename__)
        if not fiori_table_exists:
            logger.warning(f"Fiori data table not found for client '{client_name}'. Continuing without Fiori data.")

    except Exception as e:
        print(f"Error getting dynamic models for client '{client_name}': {e}")
        logger.error(f"Error getting dynamic models for client '{client_name}': {e}", exc_info=True)
        return {"error": f"Invalid client name or configuration error for '{client_name}'", "details": str(e),
                "status_code": 400}

    await create_table(engine, RequestArray)
    await create_table(engine, LicenseOptimizationResult)

    RequestArray1 = RequestArray
    LicenseOptimizationResult1 = LicenseOptimizationResult
    request_id = f"REQ-{uuid.uuid4()}"

    new_request = RequestArray1(
        req_id=request_id,
        CLIENT_NAME=client_name,
        SYSTEM_NAME=system_id,
        STATUS="IN_PROGRESS"
    )
    db.add(new_request)
    db.commit()
    db.refresh(new_request)
    req_id = new_request.req_id

    results = {}

    try:
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
                logger.warning(
                    f"Could not apply ratio filter due to data format issue or DB error: {ratio_e}",
                    exc_info=True)
                print(f"Warning: Could not apply ratio filter due to data format issue or DB error: {ratio_e}")

        if role_names:
            logger.debug(f"Filtering by specific roles: {role_names}")
            query = query.filter(_BaseLiceData.AGR_NAME.in_(role_names))

        roles_data = query.all()
        if not roles_data:
            msg = f"No roles found matching criteria for client '{client_name}'"
            if role_names: msg += f" and specific roles {role_names}"
            logger.info(msg)

            return {"message": msg, "status_code": 404}



        distinct_roles = sorted(list({r.AGR_NAME for r in roles_data}))


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

                    # Group by app (TITLE_SUBTITLE_INFORMATION) and collect actions
                    fiori_apps_dict = {}
                    for fiori_record in fiori_query:
                        app_name = fiori_record.TITLE_SUBTITLE_INFORMATION
                        action = fiori_record.ACTION

                        if app_name and app_name.strip():  # Only if app name is not empty
                            if app_name not in fiori_apps_dict:
                                fiori_apps_dict[app_name] = {"app": app_name, "actions": []}

                            if action and action.strip() and action not in fiori_apps_dict[app_name]["actions"]:
                                fiori_apps_dict[app_name]["actions"].append(action)

                    # Convert dict to list
                    fiori_apps = list(fiori_apps_dict.values())
                    logger.debug(f"Found {len(fiori_apps)} Fiori apps for role '{role}'")

                except Exception as fiori_e:
                    logger.warning(f"Error fetching Fiori data for role '{role}': {fiori_e}", exc_info=True)
                    fiori_apps = []

            # UPDATED: Include Fiori apps in the role JSON
            role_json = {
                "role": role,
                "currentLicense": target_license,
                "authorizationObjects": authorization_objects,
                "transactionCodes": transaction_codes,
                "fioriApps": fiori_apps  # NEW: Added Fiori apps
            }

            # role_json = {
            #     "role": role, "currentLicense": target_license,
            #     "authorizationObjects": authorization_objects,
            #     "transactionCodes": transaction_codes
            # }
            prompt = f"""
I’m optimizing SAP FUE license consumption for an SAP role.
Client: {client_name}
SAP System ID:{system_id}
SAP System Info: {sap_system_info}

Here’s the role data in JSON:

{json.dumps(role_json, indent=2)}

Task: Check if these authorization objects are required for the role’s intended functions, based on the listed transaction codes. Suggest changes to reduce FUE license consumption (e.g., to GC Core Use or GC Self Service Use) by adjusting or removing objects.

Please provide the response in this exact JSON format for each authorization object.Ensure that data is provided for every field in the JSON structure below.:
[
  {{"authorizationObject": "string", "field": "string", "value": "string", "licenseCanBeReduced": "Yes/No/May Be", "insights": "Short reason why reduction is possible or not", "recommendation": "Short fix to lower license consumption", "explanation": "Detailed explanation of the analysis"}}
]
Output ONLY the valid JSON array. No additional text or markdown formatting.

"""
            try:
                logger.info(f"prompt: {prompt}")
                ai_response = call_ai_api(prompt)
                print(ai_response)
                logger.debug(f"Ollama API response for role '{role}': {ai_response}")
                if ai_response.startswith("```json") or ai_response.startswith("```"):
                    ai_response = ai_response.strip("```json").strip("```").strip()

                authorization_analysis = json.loads(ai_response)

                for item in authorization_analysis:
                    db_result = LicenseOptimizationResult1(
                        REQ_ID=req_id,
                        ROLE_ID=role,
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
                logger.error(f"Failed to parse AI response for role '{role}': {str(e)}", exc_info=True)
                authorization_analysis = [{
                    "error": f"Failed to parse AI response for role '{role}': {str(e)}"
                }]

            results[role] = authorization_analysis
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
            new_request.STATUS = "FAILED"
            db.commit()
            return {"error": f"Error writing results to file: {e}", "status_code": 500}

        new_request.STATUS = "COMPLETED"
        db.commit()
        return results


    except Exception as e:
        print(
            f"Unexpected error during license optimization query/processing for client {client_name} and system id {system_id}: {e}")
        logger.error(
            f"Unexpected error during license optimization query/processing for client {client_name} and system id {system_id}: {e}",
            exc_info=True)
        traceback.print_exc()
        if new_request:
            new_request.STATUS = "FAILED"
            db.commit()

        return {"error": f"An unexpected server error occurred during optimization.", "details": str(e),
                "status_code": 500}





