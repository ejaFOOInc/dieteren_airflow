import struct
import logging
import pyodbc
import requests

from . import auth
from . import sql_conn

from azure.identity import ClientSecretCredential
from airflow.hooks.base import BaseHook
from airflow.sensors.python import PythonSensor

def run_python_sensor(
    task_id :str,
    token :str,
    sql_server :str,
    database :str,
    query :str,
    file_count_limit :int
):

    return  PythonSensor(
        task_id = task_id,
        python_callable = sensor_function,
        op_kwargs = {
            'token': token,
            'server': sql_server,
            'database': database,
            'query': query,
            'file_count_limit': file_count_limit
        },
        # mode = 'reschedule',
        poke_interval = 5,
        timeout = 3600
    )

def sensor_function(
    token :str,
    server :str,
    database :str,
    query :str,
    file_count_limit :int,
    **context
):
    """
    
    Args:
        token (string): security-token
        server (string): 
        database (string):
        query (string):
    """

    logger = logging.getLogger("airflow.task")

    isEncryptedConnection = "yes"
    isTrustedServerCertificate = "no"
    connectionTimeout = 30

    SQL_COPT_SS_ACCESS_TOKEN = 1256
    try:
        conn_str = sql_conn.get_connection_string(server, database, isEncryptedConnection, isTrustedServerCertificate, connectionTimeout)
        logger.info(f"Successfully created SQL Server Connection string for {server}.{database}")
    except Exception as e:
        logger.error(f"FAILED to create connection string for {server}.{database}")

    try:
        logger.info(f"Connecting for execution - to Fabric SQL: {server}.{database}")
        with pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token}) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logging_result = "Polling condition has been met." if (count == 1) else "BUT polling condition has not been met!"
                logger.info(f"Query successful. {logging_result}")
                return count == 1
    except Exception as e:
        logger.error(f"FAILED Database connection: {str(e)}\n{conn_str}")
        raise