from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricPipelineJobParameters
# from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from airflow.models import Variable

from sqldb_helpers import sensor_function
from callback import success_callback, failure_callback

# =============================================================================
# Configuration
# =============================================================================
# DV connection
ENV = 'dv' # development
# FABRIC_CONN_ID = "demo-fabric-tenant"
FABRIC_CONN_ID = Variable.get("dv_envar_fabric_conn_id")
# QA connection
# FABRIC_CONN_ID = "fabric-tenant-QA"

# =============================================================================
# Configuration - polling system details
# =============================================================================
# WS_Technical_DV.DB-Technical
SQL_SERVER = "kf3ifkwnxglehp4wd4umfiuaxq-zdvvqhc3wrwebmpjjvnu4cbi7i.database.fabric.microsoft.com" 
SQL_DATABASE = "DB_Technical-16d208af-97ef-40bb-97bf-444c7816c8a8"
TABLE_NAME = "[input].[airflow_test]"

# =============================================================================
# Configuration - Fabric Pipeline details
# =============================================================================
# WS_Analytical_Raw_DV
WORKSPACE_ID = "80e57629-b40d-45e0-87f8-ef1f770aeffe"
# WS_Analytical_Raw_DV.Source.SALESFORCE.PL_Load_SALESFORCE
PL_Load_SALESFORCE_ID = "215a7e9e-fc51-45a8-bc21-8f0849455934"
# WS_Analytical_Raw_DV.Source.OLIVIA.PL_Load_OLIVIA
PL_Load_OLIVIA_ID = "ab63ed58-6a39-40bf-97ce-b59699c7e64c"
# WS_Analytical_Raw_QA
# WORKSPACE_ID = "32cf9972-bc27-440d-8389-f343208faa1d"
# WS_Analytical_Raw_QA.Source.SALESFORCE.PL_Load_SALESFORCE
# PL_Load_SALESFORCE_ID = "273b4067-827c-4acf-b783-06a97b92abb6"
# WS_Analytical_Raw_QA.Source.OLIVIA.PL_Load_OLIVIA
# PL_Load_OLIVIA_ID = "b3620fef-edcd-437d-8287-9fbe34e27ea2"

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
    on_success_callback=success_callback,
    on_failure_callback=failure_callback,
    tags=['fabric', 'dbt', ENV]
) as dag:

    # ===================================================
    # BRANCH SENSOR
    # ===================================================

    wait_for_files = PythonSensor(
        task_id = 'wait_for_sql_data',
        python_callable = sensor_function,
        op_kwargs = {
            'FABRIC_CONN_ID':FABRIC_CONN_ID,
            'server': SQL_SERVER,
            'database': SQL_DATABASE,
            'table_name': TABLE_NAME
        },
        # mode = 'reschedule',
        poke_interval = 5,
        timeout = 3600
    )
    
    # ===================================================
    # BRANCH SALESFORCE
    # ===================================================

    run_pipeline_SALESFORCE = MSFabricRunJobOperator(
        task_id="runPipelineTaskSALESFORCE",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,  # WS_Analytical_Raw_[DV, QA]
        item_id=PL_Load_SALESFORCE_ID,  # PL_Load_SALESFORCE
        job_type="Pipeline",
        timeout=600,
        deferrable=True,
        job_params= MSFabricPipelineJobParameters()
            .set_parameter("SourceName", "SALESFORCE")
            .set_parameter("ApplicationName", "SALESFORCE")
            .set_parameter("ObjectName", "[object Object]")
            .to_json()
    )

    # ===================================================
    # BRANCH OLIVIA
    # ===================================================

    run_pipeline_OLIVIA= MSFabricRunJobOperator(
        task_id="runPipelineTaskOLIVIA",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,  # WS_Analytical_Raw_[DV, QA]
        item_id=PL_Load_OLIVIA_ID,  # PL_Load_OLIVIA
        job_type="Pipeline",
        timeout=600,
        deferrable=True,
        job_params= MSFabricPipelineJobParameters()
            .set_parameter("SourceName", "OLIVIA")
            .set_parameter("ApplicationName", "OLIVIA")
            .set_parameter("ObjectName", "[object Object]")
            .to_json()
    )

    # ===================================================
    # BRANCH dbt refresh
    # ===================================================

    DBT_ACCOUNT_ID = "175956220182744"
    DBT_OLIVIA_DM_JOB_ID = "175956220200805" 
    
    # dbt_job_run = DbtCloudRunJobOperator(
        #task_id = "run_dbt_job",

        ## Airflow connection
        #dbt_cloud_conn_id = "dbt_cloud",

        ## dbt Cloud job
        #job_id = DBT_OLIVIA_DM_JOB_ID,

        ## CLEAN PRODUCTION SETTINGS
        #wait_for_termination = True,
        #check_interval = 60,
        #timeout = 600,

        ## saving workers
        # deferrable = True,

        ## observability
        ## additional_run_config = {
        ##     "cause": "Triggered by Airflow"
        ## }
        #trigger_reason = "Triggered from Fabric Airflow"
    #)

    # ===================================================
    # PARALLEL EXECUTION
    # ===================================================

    wait_for_files >> [run_pipeline_SALESFORCE, run_pipeline_OLIVIA]# >> dbt_job_run
    
