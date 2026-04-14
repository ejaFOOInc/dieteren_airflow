import logging

from datetime import datetime, timedelta
from airflow import DAG
# from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator

# from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator
# from airflow.providers.microsoft.fabric.operators.run_item import MSFabricPipelineJobParameters
# from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from airflow.models import Variable

from plugins.config_logger import config_logger
from plugins.callback import success_callback, failure_callback
from plugins.run_python_sensor import run_python_sensor
from plugins.fabric_run_pipeline import fabric_run_pipeline
# from plugins.run_dbt_job import dbt_run_job

from plugins import auth

# =============================================================================
# Configuration - polling system details
# =============================================================================

SQL_SERVER = Variable.get(f"sqlserver_dbtechnical")
SQL_DATABASE = Variable.get(f"sqlserver_dbtechnical_database")
# No more need for the table name in the variables
# replaced by QUERY_[source] in sensor task definitions
# TABLE_NAME = Variable.get(f"sqlserver_dbtechnical_databasetable")

# =============================================================================
# Configuration - Fabric Pipeline details
# =============================================================================

WORKSPACE_ID = Variable.get(f"airflow_workspace_id")

PL_Load_SALESFORCE_ID = Variable.get(f"pipeline_Salesforce_id")
PL_Load_OLIVIA_ID = Variable.get(f"pipeline_Olivia_id")

# =============================================================================
# Configuration
# =============================================================================

ENV = Variable.get("ENV")
TENANT_CONN = 'fabric_conn'

# logger = logging.getLogger("airflow.task")
# logger.info("/nUse deferred for this task: xxxxxxx/n")

# print(f"1 - test message, starting for dag in environment: {ENV}")

# def hello_world():
    # print(f"test message, starting for dag in environment: {ENV}")
    # logger.info("/nUse deferred for this task: xxxxxxx/n") # <-- logging only works inside of def
    # logger.info("Hello World, this is output from Fabric Managed Airflow!")

# getting the token
TOKEN = auth.get_auth_token(connection=TENANT_CONN)

# =============================================================================
# DAG Definition
# =============================================================================
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id=f'DAG_Oliva_{ENV}',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="30 7 * * *",
    catchup=False,
    # on_success_callback=success_callback,
    # on_failure_callback=failure_callback,
    tags=['fabric', 'dbt', ENV]
) as dag:

    # ===================================================
    # TASK CONFIG LOG
    # =================================================== 

    log_config = PythonOperator(
        task_id = "Log_config",
        python_callable = config_logger,
        op_kwargs = {
            "environment": ENV
        }
    )

    # ===================================================
    # TASK SENSOR - OLIVIA
    # ===================================================
    QUERY_SENSOR_OLIVIA = """
    WITH REF AS (
        SELECT 'ACCSXXT' AS [ObjectName]
        UNION SELECT 'CARCNFT'
        UNION SELECT 'CFRPRXT'
        UNION SELECT 'CUSTXXT'
        UNION SELECT 'DSCNTXT'
        UNION SELECT 'EDI_TRIGGERS'
        UNION SELECT 'EQUIPXT'
        UNION SELECT 'INCENTT'
        UNION SELECT 'NADIN_INVALID_PRICES'
        UNION SELECT 'OFORXXT'
        UNION SELECT 'PRICEXT'
    )

    SELECT
        COUNT(1)
    FROM
        [input].[ImportTableHistory] I
    INNER JOIN REF
        ON I.[ObjectName] = REF.[ObjectName]
    WHERE
        I.[SourceName] = 'OLIVIA'
        AND CAST(I.[LoadDateTime] AS DATE) = CAST(GETDATE() -1 AS DATE)
    ORDER BY
        I.[ObjectName]
    """
    wait_for_olivia_data = run_python_sensor(
        task_id = "wait_for_olivia_data",
        token = TOKEN,
        sql_server = SQL_SERVER,
        database = SQL_DATABASE,
        query = QUERY_SENSOR_OLIVIA,
        file_count_limit = 2
    )

    # ===================================================
    # TASK SENSOR - SALESFORCE
    # ===================================================
    QUERY_SENSOR_SALESFORCE = """
    SELECT 1
    """
    wait_for_salesforce_data = run_python_sensor(
        task_id = "wait_for_salesforce_data",
        token = TOKEN,
        sql_server = SQL_SERVER,
        database = SQL_DATABASE,
        query = QUERY_SENSOR_SALESFORCE,
        file_count_limit = 2
    )
    
    # ===================================================
    # TASK SALESFORCE
    # ===================================================

    # run_pipeline_SALESFORCE = fabric_run_pipeline(
    #     task_id="runPipelineTaskSALESFORCE",
    #     fabric_conn_id=FABRIC_CONN_ID,
    #     workspace_id=WORKSPACE_ID,
    #     item_id=PL_Load_SALESFORCE_ID,
    #     timeout=600,
    #     deferrable=False,#Variable.get("USE_DEFERRABLE"),
    #     parm_SourceName="SALESFORCE",
    #     parm_ApplicationName="SALESFORCE"
    # )

    # ===================================================
    # TASK OLIVIA
    # ===================================================

    # run_pipeline_OLIVIA = fabric_run_pipeline(
    #     task_id="runPipelineTaskOLIVIA",
    #     fabric_conn_id=FABRIC_CONN_ID,
    #     workspace_id=WORKSPACE_ID,
    #     item_id=PL_Load_OLIVIA_ID,
    #     timeout=600,
    #     deferrable=False,#Variable.get("USE_DEFERRABLE"),
    #     parm_SourceName="OLIVIA",
    #     parm_ApplicationName="OLIVIA"
    # )

    # ===================================================
    # TASK dbt refresh OLIVIA
    # ===================================================

    # UNCOMMENT AFTER TESTING
    # DBT_ACCOUNT_ID = Variable.get("dbt_account_id") ## unused - to be removed
    # DBT_OLIVIA_DM_JOB_ID = Variable.get("dbt_Olivia_DM_jobid")

    # dbt_job =  dbt_run_job(
    #     task_id="run_olivia_dm",
    #     conn_id="dbt_cloud",
    #     job_id=DBT_OLIVIA_DM_JOB_ID,
    #     defferable=False,
    #     trigger_reason="Triggered by Airflow"
    # )

    # dbt_job_run = DbtCloudRunJobOperator(
    #     task_id = "run_dbt_job",

    #     ## Airflow connection
    #     dbt_cloud_conn_id = "dbt_cloud",

    #     ## dbt Cloud job
    #     job_id = DBT_OLIVIA_DM_JOB_ID,

    #     ## CLEAN PRODUCTION SETTINGS
    #     wait_for_termination = True,
    #     check_interval = 60,
    #     timeout = 600,

    #     ## saving workers
    #     deferrable = False,

    #     ## observability
    #     ## additional_run_config = {
    #     ##     "cause": "Triggered by Airflow"
    #     ## }
    #     trigger_reason = "Triggered from Fabric Airflow"
    # )

    # ===================================================
    # PARALLEL EXECUTION
    # ===================================================

    [wait_for_olivia_data, wait_for_salesforce_data] #>> [run_pipeline_SALESFORCE, run_pipeline_OLIVIA]# >> dbt_job_run
    
