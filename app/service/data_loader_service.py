import xml.etree.ElementTree as ET
import csv
from io import BytesIO
from sqlalchemy.orm import Session
from app.core.logger import setup_logger, get_daily_log_filename
from app.models.dynamic_models import (
    create_lice_data_model,
    create_auth_data_model,
    ensure_table_exists, create_role_fiori_data_model, create_role_master_derived_data, create_user_role_data,
    create_user_data, create_role_lic_summary_data_model, create_user_role_mapping_data_model,
    create_auth_obj_field_lic_data
)
from app.models.database import engine
logger = setup_logger("app_logger")
class DataLoaderError(Exception):
    pass

async def load_lice_data_from_xml_upload(db: Session, xml_file, client_name: str, system_name: str):
    if not xml_file:
        logger.info(f"Skipping XML data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No XML file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting XML data load for client: {client_name}, system: {system_name}")

    DynamicLiceModel = create_lice_data_model(client_name, system_name)
    table_name = DynamicLiceModel.__tablename__
    ensure_table_exists(engine, DynamicLiceModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicLiceModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}")
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    try:
        xml_content = xml_file.read()
        tree = ET.parse(BytesIO(xml_content))
        root = tree.getroot()
        namespaces = {'asx': 'http://www.sap.com/abapxml'}
        items = root.findall('.//asx:values/DOWNLOAD/item', namespaces)
        if not items:
            logger.warning("No <item> elements found in XML.")
            raise DataLoaderError("No <item> elements found in XML.")

        roles_info = {}
        objects_to_load = []
        for item in items:
            agr_name = item.findtext('AGR_NAME', '')
            auth_obj = item.findtext('OBJECT', '')
            logger.debug(f"Processing item for AGR_NAME: '{agr_name}', OBJECT: '{auth_obj}'")

            if not auth_obj:
                agr_classif_check = item.findtext('AGR_CLASSIF')
                if agr_name and agr_classif_check is not None:
                    roles_info[agr_name] = {
                        'AGR_TEXT': item.findtext('AGR_TEXT', ''),
                        'AGR_CLASSIF': item.findtext('AGR_CLASSIF', ''),
                        'AGR_RATIO': item.findtext('AGR_RATIO', ''),
                        'AGR_OBJECTS': item.findtext('AGR_OBJECTS', '0'),
                        'AGR_USERS': item.findtext('AGR_USERS', '0')
                    }
            else:
                role_specific_info = roles_info.get(agr_name, {})
                lice_data_obj = DynamicLiceModel(
                    AGR_NAME=agr_name, OBJECT=auth_obj,
                    TTEXT=item.findtext('TTEXT', ''), FIELD=item.findtext('FIELD', ''),
                    LOW=item.findtext('LOW', ''), HIGH=item.findtext('HIGH', ''),
                    CLASSIF_S4=item.findtext('CLASSIF_S4', ''),
                    AGR_TEXT=role_specific_info.get('AGR_TEXT', item.findtext('AGR_TEXT', '')),
                    AGR_CLASSIF=role_specific_info.get('AGR_CLASSIF', item.findtext('AGR_CLASSIF', '')),
                    AGR_RATIO=role_specific_info.get('AGR_RATIO', item.findtext('AGR_RATIO', '')),
                    AGR_OBJECTS=role_specific_info.get('AGR_OBJECTS', item.findtext('AGR_OBJECTS', '0')),
                    AGR_USERS=role_specific_info.get('AGR_USERS', item.findtext('AGR_USERS', '0'))
                )
                objects_to_load.append(lice_data_obj)

        if not objects_to_load:
            logger.warning("No valid object data found in XML.")
            raise DataLoaderError("No valid object data found.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}


    except ET.ParseError as e:
        db.rollback()
        logger.error(f"XML ParseError: {e}", exc_info=True)
        raise DataLoaderError(f"XML ParseError: {e}")

    except Exception as e:
        db.rollback()
        logger.error(f"Failed loading XML data: {e}", exc_info=True)
        raise DataLoaderError(f"Failed loading XML data: {e}")



async def load_auth_data_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """Parses Role Auth CSV from a file-like object, ensures table exists, truncates, and loads data."""
    if not csv_file:
        logger.info(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting CSV data load for client: {client_name}, system: {system_name}")


    DynamicAuthModel = create_auth_data_model(client_name, system_name) # Pass system_name
    table_name = DynamicAuthModel.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicAuthModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicAuthModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()

        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
                logger.debug("Successfully decoded CSV content with 'utf-8-sig'.")

            except UnicodeDecodeError:

                csv_text = BytesIO(csv_content).read().decode('cp1252')
                logger.debug("Successfully decoded CSV content with 'cp1252'.")

        csv_reader = csv.reader(csv_text.splitlines())
        headers = ['agr_name', 'object', 'auth_name', 'field_name', 'auth_value_low', 'auth_value_high']
        next(csv_reader)



        field_map = {
            'AGR_NAME': 0,
            'OBJECT': 1,
            'AUTH_NAME': 2,
            'FIELD_NAME': 3,
            'AUTH_VALUE_LOW': 4,
            'AUTH_VALUE_HIGH': 5
        }

        for i, row in enumerate(csv_reader):
            try:
                obj_data = {model_field: row[csv_index]
                            for model_field, csv_index in field_map.items()}
                auth_data_obj = DynamicAuthModel(**obj_data)
                objects_to_load.append(auth_data_obj)
            except IndexError as e:
                logger.error(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                print(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                raise DataLoaderError(f"Error processing row {i+2}: Not enough columns.")
            except Exception as row_e:
                print(f"Error processing row {i+2} in CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i+2} in CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i+2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in CSV data.")
            print(f"Warning: No data rows found in CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading CSV data: {e}")
        raise DataLoaderError(f"Failed loading CSV data: {e}")




async def load_role_fiori_map_data_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """Parses Role Auth CSV from a file-like object, ensures table exists, truncates, and loads data."""
    if not csv_file:
        logger.info(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting CSV data load for client: {client_name}, system: {system_name}")


    DynamicRoleFioriDataModel = create_role_fiori_data_model(client_name, system_name) # Pass system_name
    table_name = DynamicRoleFioriDataModel.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicRoleFioriDataModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicRoleFioriDataModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()
        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
            except UnicodeDecodeError:
                csv_text = BytesIO(csv_content).read().decode('cp1252')
        csv_reader = csv.reader(csv_text.splitlines())
        headers = ['role', 'role_description','sematic_object', 'action', 'title_subtitle_information', 'application_type', 'application_resources','sap_fiori_id','tile_title']
        next(csv_reader)


        field_map = {
            'ROLE': 0,
            'ROLE_DESCRIPTION': 1,
            'TILE_TARGET_MAPPING_MATCHING_TEXT': 2,
            'SEMANTIC_OBJECT': 3,
            'ACTION': 4,
            'TITLE_SUBTITLE_INFORMATION': 5,
            'APPLICATION_TYPE': 6,
            'APPLICATION_RESOURCES': 7,
            'SAP_FIORI_ID' :8,
            'APPLICATION_COMPONENT_ID':9,
            'ODATA_SERVICE_NAME' :10,
            'CATALOG_ID' :11,
            'CATALOG_TITLE':12
        }

        for i, row in enumerate(csv_reader):
            try:
                obj_data = {model_field: row[csv_index]
                            for model_field, csv_index in field_map.items()}
                auth_data_obj = DynamicRoleFioriDataModel(**obj_data)
                objects_to_load.append(auth_data_obj)
            except IndexError as e:
                logger.error(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                print(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                raise DataLoaderError(f"Error processing row {i+2}: Not enough columns.")
            except Exception as row_e:
                print(f"Error processing row {i+2} in CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i+2} in CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i+2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in CSV data.")
            print(f"Warning: No data rows found in CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading CSV data: {e}")
        raise DataLoaderError(f"Failed loading CSV data: {e}")


async def load_master_derived_role_data_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """Parses Role Auth CSV from a file-like object, ensures table exists, truncates, and loads data."""
    if not csv_file:
        logger.info(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting CSV data load for client: {client_name}, system: {system_name}")


    DynamicMasterDerivedDataModel = create_role_master_derived_data(client_name, system_name) # Pass system_name
    table_name = DynamicMasterDerivedDataModel.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicMasterDerivedDataModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicMasterDerivedDataModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()
        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
            except UnicodeDecodeError:
                csv_text = BytesIO(csv_content).read().decode('cp1252')
        csv_reader = csv.reader(csv_text.splitlines())
        headers = ['derived_role', 'master_role']
        next(csv_reader)
        field_map = {
            'DERIVED_ROLE': 0,
            'MASTER_ROLE': 1
        }

        for i, row in enumerate(csv_reader):
            try:
                obj_data = {model_field: row[csv_index]
                            for model_field, csv_index in field_map.items()}
                auth_data_obj = DynamicMasterDerivedDataModel(**obj_data)
                objects_to_load.append(auth_data_obj)
            except IndexError as e:
                logger.error(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                print(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                raise DataLoaderError(f"Error processing row {i+2}: Not enough columns.")
            except Exception as row_e:
                print(f"Error processing row {i+2} in CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i+2} in CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i+2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in CSV data.")
            print(f"Warning: No data rows found in CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading CSV data: {e}")
        raise DataLoaderError(f"Failed loading CSV data: {e}")


async def load_user_data_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """Parses Role Auth CSV from a file-like object, ensures table exists, truncates, and loads data."""
    if not csv_file:
        logger.info(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting CSV data load for client: {client_name}, system: {system_name}")

    DynamicUserDataModel = create_user_data(client_name, system_name) # Pass system_name
    table_name = DynamicUserDataModel.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicUserDataModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicUserDataModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()
        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
            except UnicodeDecodeError:
                csv_text = BytesIO(csv_content).read().decode('cp1252')
        csv_reader = csv.reader(csv_text.splitlines())
        headers = ['user', 'full_name','id',
                   'current_classification','target_classification','ratio','ref_user','user_group','last_logon','count']
        next(csv_reader)
        field_map = {
            'USER': 0,
            'FULL_NAME': 1,
            'ID':2,
            'CURRENT_CLASSIFICATION':3,
            'TARGET_CLASSIFICATION':4,
            'RATIO':5,
            'REF_USER':6,
            'USER_GROUP':7,
            'LAST_LOGON':8,
            'COUNT':9
        }

        for i, row in enumerate(csv_reader):
            try:
                obj_data = {model_field: row[csv_index]
                            for model_field, csv_index in field_map.items()}
                auth_data_obj = DynamicUserDataModel(**obj_data)
                objects_to_load.append(auth_data_obj)
            except IndexError as e:
                logger.error(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                print(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                raise DataLoaderError(f"Error processing row {i+2}: Not enough columns.")
            except Exception as row_e:
                print(f"Error processing row {i+2} in CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i+2} in CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i+2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in CSV data.")
            print(f"Warning: No data rows found in CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading CSV data: {e}")
        raise DataLoaderError(f"Failed loading CSV data: {e}")

async def load_user_role_map_data_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """Parses Role Auth CSV from a file-like object, ensures table exists, truncates, and loads data."""
    if not csv_file:
        logger.info(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting CSV data load for client: {client_name}, system: {system_name}")

    DynamicUserRoleDataModel = create_user_role_data(client_name, system_name) # Pass system_name
    table_name = DynamicUserRoleDataModel.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicUserRoleDataModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicUserRoleDataModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()
        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
            except UnicodeDecodeError:
                csv_text = BytesIO(csv_content).read().decode('cp1252')
        csv_reader = csv.reader(csv_text.splitlines())
        headers = ['role', 'user_name']
        next(csv_reader)
        field_map = {
            'ROLE': 0,
            'USER_NAME': 1
        }

        for i, row in enumerate(csv_reader):
            try:
                obj_data = {model_field: row[csv_index]
                            for model_field, csv_index in field_map.items()}
                auth_data_obj = DynamicUserRoleDataModel(**obj_data)
                objects_to_load.append(auth_data_obj)
            except IndexError as e:
                logger.error(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                print(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                raise DataLoaderError(f"Error processing row {i+2}: Not enough columns.")
            except Exception as row_e:
                print(f"Error processing row {i+2} in CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i+2} in CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i+2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in CSV data.")
            print(f"Warning: No data rows found in CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading CSV data: {e}")
        raise DataLoaderError(f"Failed loading CSV data: {e}")


async def load_user_role_mapping_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """
    Loads UserRoleMapping CSV with AGR_NAME and UNAME, then fills LICENSE_MAPPED_TO_ROLE using VLOOKUP.
    CSV should have: AGR_NAME, UNAME, LICENSE_MAPPED_TO_ROLE (can be empty - will be filled via VLOOKUP)
    """
    if not csv_file:
        logger.info(
            f"Skipping User Role Mapping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(
            f"Skipping User Role Mapping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting User Role Mapping CSV data load for client: {client_name}, system: {system_name}")

    DynamicUserRoleMappingModel = create_user_role_mapping_data_model(client_name, system_name)
    table_name = DynamicUserRoleMappingModel.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicUserRoleMappingModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicUserRoleMappingModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()
        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
            except UnicodeDecodeError:
                csv_text = BytesIO(csv_content).read().decode('cp1252')

        csv_reader = csv.reader(csv_text.splitlines())
        headers = ['agr_name', 'uname', 'license_mapped_to_role']
        next(csv_reader)  # Skip header row

        field_map = {
            'AGR_NAME': 0,
            'UNAME': 1

        }

        for i, row in enumerate(csv_reader):
            try:
                # Handle case where LICENSE_MAPPED_TO_ROLE column might be empty or missing
                agr_name = row[0] if len(row) > 0 else ''
                uname = row[1] if len(row) > 1 else ''

                mapping_obj = DynamicUserRoleMappingModel(
                    AGR_NAME=agr_name,
                    UNAME=uname,
                )
                objects_to_load.append(mapping_obj)
            except Exception as row_e:
                print(f"Error processing row {i + 2} in CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i + 2} in CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i + 2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in CSV data.")
            print(f"Warning: No data rows found in CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} user role mapping records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading User Role Mapping CSV data: {e}")
        raise DataLoaderError(f"Failed loading User Role Mapping CSV data: {e}")

async def load_role_lic_summary_data_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """
    Parses Role License Summary CSV from a file-like object, ensures table exists, truncates, and loads data.
    Expected CSV columns: ROLE, TARGET_CLASSIFICATION, and other relevant fields
    """
    if not csv_file:
        logger.info(
            f"Skipping Role Lic Summary CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(
            f"Skipping Role Lic Summary CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting Role Lic Summary CSV data load for client: {client_name}, system: {system_name}")

    DynamicRoleLicSummaryModel = create_role_lic_summary_data_model(client_name, system_name)
    table_name = DynamicRoleLicSummaryModel.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicRoleLicSummaryModel)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicRoleLicSummaryModel).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()
        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
            except UnicodeDecodeError:
                csv_text = BytesIO(csv_content).read().decode('cp1252')

        csv_reader = csv.reader(csv_text.splitlines())
        headers = next(csv_reader)  # Read the header row to get actual column names

        # Define expected field mapping - adjust based on your actual CSV structure
        # You may need to modify this based on your actual CSV column structure
        field_map = {
            'ROLE': 0,  # Assuming first column is role name
            'ROLE_DESCRIPTION':1,
            'TARGET_CLASSIFICATION': 2,
            'RATIO' :3,
            'OBJECTS' : 4,
            'COUNT' : 5,
            'USERS' :6
        # Assuming second column is target classification
            # Add more fields as needed based on your role_lic_summary model
        }

        for i, row in enumerate(csv_reader):
            try:
                # Create object with mapped fields
                obj_data = {}
                for model_field, csv_index in field_map.items():
                    if csv_index < len(row):
                        obj_data[model_field] = row[csv_index].strip()
                    else:
                        obj_data[model_field] = ''  # Default empty value if column missing

                role_lic_summary_obj = DynamicRoleLicSummaryModel(**obj_data)
                objects_to_load.append(role_lic_summary_obj)

            except Exception as row_e:
                print(f"Error processing row {i + 2} in Role Lic Summary CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i + 2} in Role Lic Summary CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i + 2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in Role Lic Summary CSV data.")
            print(f"Warning: No data rows found in Role Lic Summary CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} role license summary records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading Role Lic Summary CSV data: {e}")
        raise DataLoaderError(f"Failed loading Role Lic Summary CSV data: {e}")


async def create_and_populate_role_obj_lic_sim_table(
        db: Session,
        client_name: str,
        system_name: str
):
    """
    Create role object license simulation table and populate it with data
    from the existing license table.
    """
    from app.models.dynamic_models import (
        create_lice_data_model,
        create_role_obj_lic_sim_model
    )
    logger.info(f"Starting to create and populate license simulation table for client: '{client_name}', system: '{system_name}'.")


    # Get the source model (existing license table)
    SourceLiceModel = create_lice_data_model(client_name, system_name)

    # Get the target model (simulation table)
    TargetSimModel = create_role_obj_lic_sim_model(client_name, system_name)

    # Ensure both tables exist
    ensure_table_exists(engine, SourceLiceModel)
    ensure_table_exists(engine, TargetSimModel)

    source_table_name = SourceLiceModel.__tablename__
    target_table_name = TargetSimModel.__tablename__
    logger.debug(f"Source table '{source_table_name}' and target table '{target_table_name}' verified.")

    try:
        # Clear existing data in simulation table
        deleted_count = db.query(TargetSimModel).delete()
        print(f"Cleared {deleted_count} existing records from {target_table_name}")
        logger.info(f"Cleared {deleted_count} existing records from '{target_table_name}'.")

        # Fetch all data from source table
        source_records = db.query(SourceLiceModel).all()

        if not source_records:
            msg = f"No data found in source table '{source_table_name}'. Simulation table will be empty."
            logger.warning(msg)
            return {
                "message": f"No data found in source table {source_table_name}",
                "table_name": target_table_name,
                "records_copied": 0
            }

        # Create simulation records
        sim_records = []
        for source_record in source_records:
            sim_record = TargetSimModel(
                AGR_NAME=source_record.AGR_NAME,
                AGR_TEXT=source_record.AGR_TEXT,
                OBJECT=source_record.OBJECT,
                TTEXT=source_record.TTEXT,
                FIELD=source_record.FIELD,
                LOW=source_record.LOW,
                HIGH=source_record.HIGH,
                CLASSIF_S4=source_record.CLASSIF_S4,
                OPERATION=None,  # Will be filled later
                NEW_VALUE=None,  # Will be filled later
                NEW_SIM_LICE=None,  # Will be filled later

            )
            sim_records.append(sim_record)

        # Bulk insert simulation records
        db.add_all(sim_records)
        db.commit()

        message = f"Successfully copied {len(sim_records)} records from {source_table_name} to {target_table_name}"
        logger.info(message)

        return {
            "message": message,
            "table_name": target_table_name,
            "records_copied": len(sim_records)
        }

    except Exception as e:
        db.rollback()
        error_msg = f"Failed to create/populate simulation table: {str(e)}"
        print(error_msg)
        logger.error(error_msg, exc_info=True)
        raise Exception(error_msg)


async def load_auth_obj_field_lic_data_from_csv_upload(db: Session, csv_file, client_name: str, system_name: str):
    """Parses Role Auth CSV from a file-like object, ensures table exists, truncates, and loads data."""
    if not csv_file:
        logger.info(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        print(f"Skipping CSV data load for client: {client_name}, system: {system_name} as no file was provided.")
        return {"message": "No CSV file provided, skipping load.", "table_name": None, "records_loaded": 0}

    logger.info(f"Starting CSV data load for client: {client_name}, system: {system_name}")


    DynamicAuthObjFieldLicData = create_auth_obj_field_lic_data(client_name, system_name) # Pass system_name
    table_name = DynamicAuthObjFieldLicData.__tablename__
    engine = db.bind
    ensure_table_exists(engine, DynamicAuthObjFieldLicData)
    logger.debug(f"Dynamic model and table '{table_name}' for client '{client_name}' created/verified.")

    try:
        deleted_count = db.query(DynamicAuthObjFieldLicData).delete()
        logger.info(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
        print(f"Truncated (deleted) {deleted_count} rows from {table_name}.")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to truncate table {table_name}: {e}", exc_info=True)
        raise DataLoaderError(f"Failed to truncate table {table_name}: {e}")

    objects_to_load = []
    try:
        csv_content = csv_file.read()
        try:
            csv_text = BytesIO(csv_content).read().decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                csv_text = BytesIO(csv_content).read().decode('latin-1')
            except UnicodeDecodeError:
                csv_text = BytesIO(csv_content).read().decode('cp1252')
        csv_reader = csv.reader(csv_text.splitlines())
        headers = ['authorization_object', 'field','activity', 'text', 'license', 'ui_text']
        next(csv_reader)


        field_map = {
            'AUTHORIZATION_OBJECT': 0,
            'FIELD': 1,
            'ACTIVITIY': 2,
            'TEXT': 3,
            'LICENSE': 4,
            'UI_TEXT': 5
        }

        for i, row in enumerate(csv_reader):
            try:
                obj_data = {model_field: row[csv_index]
                            for model_field, csv_index in field_map.items()}
                auth_data_obj = DynamicAuthObjFieldLicData(**obj_data)
                objects_to_load.append(auth_data_obj)
            except IndexError as e:
                logger.error(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                print(f"Error processing row {i+2} in CSV data: Not enough columns. Row: {row}")
                raise DataLoaderError(f"Error processing row {i+2}: Not enough columns.")
            except Exception as row_e:
                print(f"Error processing row {i+2} in CSV data: {row_e}")
                print(f"Row data: {row}")
                logger.error(f"Error processing row {i+2} in CSV data: {row_e}")
                raise DataLoaderError(f"Error processing row {i+2}: {row_e}")

        if not objects_to_load:
            logger.warning(f"Warning: No data rows found in CSV data.")
            print(f"Warning: No data rows found in CSV data.")

        db.add_all(objects_to_load)
        db.commit()
        msg = f"Successfully loaded {len(objects_to_load)} records into {table_name}"
        logger.info(msg)
        print(msg)
        return {"message": msg, "table_name": table_name, "records_loaded": len(objects_to_load)}

    except Exception as e:
        db.rollback()
        logger.warning(f"Failed loading CSV data: {e}")
        raise DataLoaderError(f"Failed loading CSV data: {e}")

