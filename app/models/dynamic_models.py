import re
import uuid
from app.core.logger import setup_logger
import nullable
from sqlalchemy import (
    Column, String, Integer, MetaData, Table, inspect as sqla_inspect, NotNullable
)
from app.models.database import Base, engine
logger = setup_logger("app_logger")


def clean_client_name(client_name: str) -> str:
    """Cleans the client name to be suitable for a table name prefix."""
    logger.debug(f"Received client_name: '{client_name}'")
    return re.sub(r'\W+', '', client_name.replace(' ', '_')).upper()


def clean_system_name(system_name: str) -> str:
    """Cleans the client name to be suitable for a table name prefix."""
    logger.debug(f"Received system_name: '{system_name}'")
    return re.sub(r'\W+', '', system_name.replace(' ', '_')).upper()


def clean_system_release_versionInfo(system_release_version: str) -> str:
    """Cleans the client name to be suitable for a table name prefix."""
    logger.debug(f"Received system_release_versionInfo: '{system_release_version}'")
    return re.sub(r'\W+', '', system_release_version.replace(' ', '_')).upper()




def get_lice_data_tablename(client_name: str,system_name:str) -> str:
    client = clean_client_name(client_name)
    system= clean_system_name(system_name)
    logger.info(f"Generated role object license info table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_ROLE_OBJ_LICENSE_INFO"

def get_auth_data_tablename(client_name: str,system_name:str) -> str:
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated ROLE_AUTH_OBJ_DATA table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_ROLE_AUTH_OBJ_DATA"

def get_role_fiori_data_tablename(client_name: str,system_name:str) -> str:
    client = clean_client_name(client_name)
    system= clean_system_name(system_name)
    logger.info(f"Generated ROLE_FIORI_DATA table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_ROLE_FIORI_DATA"

def get_role_master_derived_data_tablename(client_name: str,system_name:str) -> str:
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated ROLE_MASTER_DERVI_DATA table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_ROLE_MASTER_DERVI_DATA"

def get_user_data_tablename(client_name: str,system_name:str) -> str:
    client = clean_client_name(client_name)
    system= clean_system_name(system_name)
    logger.info(f"Generated USER_DATA table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_USER_DATA"

def get_user_role_data_tablename(client_name: str,system_name:str) -> str:
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated USER_ROLE_DATA table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_USER_ROLE_DATA"


def get_role_lic_summary_data_tablename(client_name: str, system_name: str) -> str:
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated ROLE_LIC_SUMMARY table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_ROLE_LIC_SUMMARY"

def get_user_role_mapping_data_tablename(client_name: str, system_name: str) -> str:
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated USER_ROLE_MAPPING table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_USER_ROLE_MAPPING"


def get_role_obj_lic_sim_tablename(client_name: str, system_name: str) -> str:
    """Generate table name for role object license simulation data."""
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated ROLE_OBJ_LIC_SIM table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_ROLE_OBJ_LIC_SIM"

def get_auth_obj_field_lic_data_tablename(client_name: str, system_name: str) -> str:
    """Generate table name for role object license simulation data."""
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated AUTH_OBJ_FIELD_LIC_DATA table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_AUTH_OBJ_FIELD_LIC_DATA"

def get_simulation_result_tablename(client_name: str, system_name: str) -> str:
    """Generate table name for role object license simulation data."""
    client = clean_client_name(client_name)
    system = clean_system_name(system_name)
    logger.info(f"Generated SIMULATION_RESULT_DATA table name for client '{client_name}' and system '{system_name}'")
    return f"Z_FUE_{client}_{system}_SIMULATION_RESULT_DATA"

class _BaseLiceData:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    AGR_NAME = Column(String, index=True)
    OBJECT = Column(String, nullable=False)
    TTEXT = Column(String)
    FIELD = Column(String)
    LOW = Column(String)
    HIGH = Column(String)
    CLASSIF_S4 = Column(String)
    AGR_TEXT = Column(String)
    AGR_CLASSIF = Column(String)
    AGR_RATIO = Column(String)
    AGR_OBJECTS = Column(String)
    AGR_USERS = Column(String)


class _BaseAuthData:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    AGR_NAME = Column(String, nullable=False, index=True)
    OBJECT = Column(String, nullable=False)
    AUTH_NAME = Column(String, nullable=False)
    FIELD_NAME = Column(String, nullable=False)
    AUTH_VALUE_LOW = Column(String)
    AUTH_VALUE_HIGH = Column(String)

class _BaseFioriRoleData:
    id= Column(Integer, primary_key=True, index=True, autoincrement=True)
    ROLE= Column(String, nullable=False, index=True)
    ROLE_DESCRIPTION = Column(String)
    TILE_TARGET_MAPPING_MATCHING_TEXT=Column(String)
    SEMANTIC_OBJECT=Column(String)
    ACTION=Column(String)
    TITLE_SUBTITLE_INFORMATION=Column(String)
    APPLICATION_TYPE=Column(String)
    APPLICATION_RESOURCES=Column(String)
    SAP_FIORI_ID=Column(String)
    APPLICATION_COMPONENT_ID=Column(String)
    ODATA_SERVICE_NAME=Column(String)
    CATALOG_ID=Column(String)
    CATALOG_TITLE=Column(String)


class _RoleMasterDerviData:
    id=Column(Integer, primary_key=True, index=True, autoincrement=True)
    DERIVED_ROLE =Column(String)
    MASTER_ROLE=Column(String)
    TEXT=Column(String)

class _UserData:
    id= Column("id", Integer, primary_key=True, index=True, autoincrement=True)
    USER=Column("USER", String, nullable=False, index=True) # Explicitly named
    FULL_NAME=Column("FULL_NAME", String) # Explicitly named
    ID=Column("ID", String) # Already correct
    CURRENT_CLASSIFICATION=Column("CURRENT_CLASSIFICATION", String) # Explicitly named
    TARGET_CLASSIFICATION=Column("TARGET_CLASSIFICATION", String) # Explicitly named
    RATIO=Column("RATIO", String) # Explicitly named
    REF_USER=Column("REF_USER", String) # Explicitly named
    USER_GROUP=Column("USER_GROUP", String) # Already correct
    LAST_LOGON=Column("LAST_LOGON", String) # Explicitly named
    COUNT=Column("COUNT", String) # Explicitly named



class _UserRoleData:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ROLE= Column(String, nullable=False, index=True)
    USER_NAME=Column(String, nullable=False)

class _RoleLicSummary:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ROLE=Column(String, nullable=False, index=True)
    ROLE_DESCRIPTION=Column(String)
    TARGET_CLASSIFICATION=Column(String)
    RATIO=Column(String)
    OBJECTS=Column(String)
    COUNT=Column(String)
    USERS=Column(String)

class UserRoleMapping:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    AGR_NAME=Column(String, nullable=False, index=True)
    UNAME=Column(String)

class _AuthObjFieldLicData:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    AUTHORIZATION_OBJECT = Column(String, index=True)
    FIELD = Column(String, nullable=False)
    ACTIVITIY = Column(String)
    TEXT = Column(String)
    LICENSE = Column(String)
    UI_TEXT= Column(String)


class _BaseRoleObjLicSimData:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    AGR_NAME = Column(String, index=True)
    AGR_TEXT = Column(String)  # Add this line for role description
    OBJECT = Column(String, nullable=False)
    TTEXT = Column(String)
    FIELD = Column(String)
    LOW = Column(String)
    HIGH = Column(String)
    CLASSIF_S4 = Column(String)
    OPERATION = Column(String)  # New column for operation type
    NEW_VALUE = Column(String)  # New column for new value
    NEW_SIM_LICE = Column(String)  # New column for new simulation license


class _SimResultData:
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    SIMULATION_RUN_ID = Column(String, index=True, default=lambda: f"SIM_REQ-{uuid.uuid4()}")
    TIMESTAMP = Column(String, index=True)
    STATUS = Column(String(20))
    CLIENT_NAME=Column(String)
    SYSTEM_NAME=Column(String)
    FUE_REQUIRED =Column(String)
    ROLES_CHANGED=Column(String)
    ROLE_DESCRIPTION = Column(String)  # Add this line
    OBJECT= Column(String)
    FIELD= Column(String)
    VALUE_LOW= Column(String)
    VALUE_HIGH= Column(String)
    OPERATION= Column(String)
    PREV_LICENSE=Column(String)
    CURRENT_LICENSE=Column(String)





_dynamic_models_cache = {}


def create_role_lic_summary_data_model(client_name: str, system_name: str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_role_lic_summary_data_tablename(client_name, system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")
    DynamicRoleLicSummaryModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}RoleLicSummary",
        (_RoleLicSummary, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    _dynamic_models_cache[table_name] = DynamicRoleLicSummaryModel
    return DynamicRoleLicSummaryModel

def create_user_role_mapping_data_model(client_name: str, system_name: str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_user_role_mapping_data_tablename(client_name, system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicUserRoleMappingModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}UserRoleMapping",
        (UserRoleMapping, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    _dynamic_models_cache[table_name] = DynamicUserRoleMappingModel
    return DynamicUserRoleMappingModel

def create_lice_data_model(client_name: str,system_name:str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_lice_data_tablename(client_name,system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")


    DynamicLiceDataModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}LiceData",
        (_BaseLiceData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    _dynamic_models_cache[table_name] = DynamicLiceDataModel
    return DynamicLiceDataModel

def create_auth_data_model(client_name: str,system_name:str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_auth_data_tablename(client_name,system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicAuthDataModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}AuthData",
        (_BaseAuthData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    _dynamic_models_cache[table_name] = DynamicAuthDataModel
    return DynamicAuthDataModel

def create_role_fiori_data_model(client_name: str,system_name:str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_role_fiori_data_tablename(client_name,system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")
    DynamicRoleFioriDataModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}RoleFioriData",
        (_BaseFioriRoleData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    _dynamic_models_cache[table_name] = DynamicRoleFioriDataModel
    return DynamicRoleFioriDataModel


def create_role_master_derived_data(client_name: str,system_name:str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_role_master_derived_data_tablename(client_name,system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicMasterDerivedDataModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}MasterDerivedData",
        (_RoleMasterDerviData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    _dynamic_models_cache[table_name] = DynamicMasterDerivedDataModel
    return DynamicMasterDerivedDataModel

def create_user_data(client_name: str,system_name:str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_user_data_tablename(client_name,system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicUserDataModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}UserData",
        (_UserData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    _dynamic_models_cache[table_name] = DynamicUserDataModel
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    return DynamicUserDataModel

def create_user_role_data(client_name: str,system_name:str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    table_name = get_user_role_data_tablename(client_name,system_name)
    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicUserRoleDataModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}UserRoleData",
        (_UserRoleData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    _dynamic_models_cache[table_name] = DynamicUserRoleDataModel
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    return DynamicUserRoleDataModel


def create_role_obj_lic_sim_model(client_name: str, system_name: str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    """Create dynamic model for role object license simulation data."""
    table_name = get_role_obj_lic_sim_tablename(client_name, system_name)

    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicRoleObjLicSimModel = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}RoleObjLicSim",
        (_BaseRoleObjLicSimData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )

    _dynamic_models_cache[table_name] = DynamicRoleObjLicSimModel
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    return DynamicRoleObjLicSimModel

def create_auth_obj_field_lic_data(client_name: str, system_name: str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    """Create dynamic model for role object license simulation data."""
    table_name = get_auth_obj_field_lic_data_tablename(client_name, system_name)

    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicAuthObjFieldLicData = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}AuthObjFieldLicData",
        (_AuthObjFieldLicData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )

    _dynamic_models_cache[table_name] = DynamicAuthObjFieldLicData
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    return DynamicAuthObjFieldLicData



def create_simulation_result_data(client_name: str, system_name: str):
    logger.debug(f"Attempting to create or retrieve model for client='{client_name}', system='{system_name}'")
    """Create dynamic model for role object license simulation data."""
    table_name = get_simulation_result_tablename(client_name, system_name)

    if table_name in _dynamic_models_cache:
        logger.info(f"Model for table '{table_name}' found in cache. Returning cached model.")
        return _dynamic_models_cache[table_name]
    logger.info(f"Model for table '{table_name}' not in cache. Creating a new dynamic model.")

    DynamicSimResultData = type(
        f"Z_FUE_{clean_client_name(client_name)}_{clean_system_name(system_name)}AuthObjFieldLicData",
        (_SimResultData, Base),
        {"__tablename__": table_name, "__table_args__": {'extend_existing': True}}
    )
    _dynamic_models_cache[table_name] = DynamicSimResultData
    logger.info(f"Successfully created dynamic model for table '{table_name}'")
    return DynamicSimResultData

def ensure_table_exists(db_engine, model_class):
    inspector = sqla_inspect(db_engine)
    table_name = model_class.__tablename__
    if not inspector.has_table(table_name):
        print(f"Table '{table_name}' not found. Creating...")
        try:
            model_class.__table__.create(bind=db_engine)
            print(f"Table '{table_name}' created.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
            raise
    else:
        print(f"Table '{table_name}' already exists.")


