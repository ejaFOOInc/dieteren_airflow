import struct
import logging
import pyodbc
import requests
from azure.identity import ClientSecretCredential
from airflow.hooks.base import BaseHook

# def get_connection(FABRIC_CONN_ID):
    # """
    
    # """
    # logger = logging.getLogger("airflow.task.get_access_token")
    #FABRIC_CONN_ID = "demo-fabric-tenant"
    # conn = BaseHook.get_connection(FABRIC_CONN_ID)
    # extra = conn.extra_dejason

    # TENANT_ID = extra.get("tenantId") or extra.get("tenant_id")
	# logger.info(f"The extracted tenantId is {TENANT_ID}")
    # CLIENT_ID = conn.login or extra.get("clientId") or extra.get("client_id")
	# logger.info(f"The extracted clientId is {CLIENT_ID}")
    # CLIENT_SECRET = conn.password or extra.get("clientSecret") or extra.get("client_secret")

    # url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    # data = {
        # "grant_type": "client_credentials",
        # "client_id": CLIENT_ID,
        # "client_secret": CLIENT_SECRET,
        # "scope": "https://api.fabric.microsoft.com/.default",
    # }

    # r = requests.post(url, data=data)
    # return r.json()["access_token"]

def get_connection_string(server :str, database :str, isEncryptedConnection :bool, isTrustedServerCertificate :bool, connectionTimeout :int):
    """
    
    """
    return (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        f"Encrypt={isEncryptedConnection};"
        f"TrustServerCertificate={isTrustedServerCertificate};"
        f"Connection Timeout={connectionTimeout};"
    )

def sensor_function(FABRIC_CONN_ID, server :str, database :str, table_name :str, file_count_limit :int, **context):
    """
    
    Args:
        server (string): 
        database (string):
        table_name (string):
    """

    logger = logging.getLogger("airflow.task")

    isEncryptedConnection = "yes"
    isTrustedServerCertificate = "no"
    connectionTimeout = 30

    try:
        # token_str = get_access_token(FABRIC_CONN_ID)
        # FABRIC_CONN_ID = "demo-fabric-tenant"
        conn = BaseHook.get_connection(FABRIC_CONN_ID)
        logger.info(f"full airflow connection: {conn}")
        extra = conn.extra_dejson
        TENANT_ID = extra.get("tenantId") or extra.get("tenant_id")
        logger.info(f"The extracted tenantId (from connection {FABRIC_CONN_ID}): {TENANT_ID}")
        CLIENT_ID = conn.login or extra.get("clientId") or extra.get("client_id")
        logger.info(f"The extracted clientId (from connection {FABRIC_CONN_ID}): {CLIENT_ID}")
        CLIENT_SECRET = conn.password or extra.get("clientSecret") or extra.get("client_secret")

        url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
        logger.info(f"The full url constructed (from connection {FABRIC_CONN_ID}): {url}")

        data = {
        	"grant_type": "client_credentials",
        	"client_id": CLIENT_ID,
        	"client_secret": CLIENT_SECRET,
        	"scope": "https://api.fabric.microsoft.com/.default"
    	}
        logger.info(f"The extra data for the request (from connection {FABRIC_CONN_ID}): {data}")
        
        r = requests.post(url, data=data)
        token_str = r.json()["access_token"]
		# Pack the token into the format required by the ODBC driver
        token_bytes = token_str.encode("utf-16-le")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s",
            len(token_bytes),
            token_bytes,
        )

        logger.info("Successfully retrieved FABRIC Connection Token")

    except Exception as e:
        logger.error(f"FAILURE to retrieve FABRIC Connection Token - for connection {FABRIC_CONN_ID}")
        raise

    #  1256 is the magic attribute ID for SQL_COPT_SS_ACCESS_TOKEN
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    try:
        conn_str = get_connection_string(server, database, isEncryptedConnection, isTrustedServerCertificate, connectionTimeout)
        logger.info(f"Successfully created SQL Server Connection string for {server}.{database}")
    except Exception as e:
        logger.error(f"FAILED to create connection string for {server}.{database}")

    try:
        logger.info(f"Connecting for execution - to Fabric SQL: {server}.{database}")
        with pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}) as conn:
            with conn.cursor() as cursor:
                query = f"SELECT COUNT(*) FROM {table_name}"
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logger.info(f"Query successful. Found {count} records!")
                return count >= file_count_limit
    except Exception as e:
        logger.error(f"FAILED Database connection: {str(e)}\n{conn_str}")
        raise
