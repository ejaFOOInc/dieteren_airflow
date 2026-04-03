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

from plugins.callback import success_callback, failure_callback
from plugins.run_python_sensor import run_python_sensor
from plugins.fabric_run_pipeline import fabric_run_pipeline
# from plugins.run_dbt_job import dbt_run_job

# =============================================================================
# GIT Synced Airflow
# =============================================================================

# =============================================================================
# Configuration 
# =============================================================================
# DV connection
ENV = 'dv' # development
# FABRIC_CONN_ID = "demo-fabric-tenant"

# UNCOMMENT AFTER TESTING
# FABRIC_CONN_ID = Variable.get(f"{ENV}_envar_fabric_conn_id")
# QA connection
# FABRIC_CONN_ID = "fabric-tenant-QA"

# =============================================================================
# Configuration - polling system details
# =============================================================================
# WS_Technical_DV.DB-Technical

# UNCOMMENT AFTER TESTING
# SQL_SERVER = Variable.get(f"{ENV}_sqlserver_dbtechnical")
# SQL_DATABASE = Variable.get(f"{ENV}_sqlserver_dbtechnical_database")
# TABLE_NAME = Variable.get(f"{ENV}_sqlserver_dbtechnical_databasetable")

# =============================================================================
# Configuration - Fabric Pipeline details
# =============================================================================
# WS_Analytical_Raw_DV

# UNCOMMENT AFTER TESTING
# WORKSPACE_ID = Variable.get(f"{ENV}_airflow_workspace_id")
# WS_Analytical_Raw_DV.Source.SALESFORCE.PL_Load_SALESFORCE

# UNCOMMENT AFTER TESTING
# PL_Load_SALESFORCE_ID = Variable.get(f"{ENV}_pipeline_Salesforce_id")
# WS_Analytical_Raw_DV.Source.OLIVIA.PL_Load_OLIVIA

# UNCOMMENT AFTER TESTING
# PL_Load_OLIVIA_ID = Variable.get(f"{ENV}_pipeline_Olivia_id")
# WS_Analytical_Raw_QA
# WORKSPACE_ID = Variable.get("qa_airflow_workspace_id")
# WS_Analytical_Raw_QA.Source.SALESFORCE.PL_Load_SALESFORCE
# PL_Load_SALESFORCE_ID = Variable.get("qa_pipeline_Salesforce_id")
# WS_Analytical_Raw_QA.Source.OLIVIA.PL_Load_OLIVIA
# PL_Load_OLIVIA_ID = Variable.get("qa_pipeline_Olivia_id")

logger = logging.getLogger("airflow.task")
logger.info("/nUse deferred for this task: xxxxxxx/n")

def hello_world():
    logger.info("/nUse deferred for this task: xxxxxxx/n")
    logger.info("Hello World, this is output from Fabric Managed Airflow!")

# =============================================================================
# DAG Definition
# =============================================================================
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
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
    # BRANCH TEST
    # ===================================================

    do_something = PythonOperator(
        task_id = "Hello_there",
        python_callable = hello_world
    )

    # ===================================================
    # BRANCH SENSOR
    # ===================================================

    # wait_for_files = run_python_sensor(
        # task_id="wait_for_sql_data",
        # conn_id=FABRIC_CONN_ID,
        # sql_server=SQL_SERVER,
        # database=SQL_DATABASE,
        # table_name=TABLE_NAME,
        # file_count_limit=2
    # )
    
    # ===================================================
    # BRANCH SALESFORCE
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
    # BRANCH OLIVIA
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
    # BRANCH dbt refresh
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

    # wait_for_files# >> [run_pipeline_SALESFORCE, run_pipeline_OLIVIA]# >> dbt_job_run
    
