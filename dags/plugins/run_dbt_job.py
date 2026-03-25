from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import logging

def dbt_run_job(
    task_id :str,
    conn_id :str,
    job_id :str,
    defferable :bool,
    trigger_reason :str
):
    
    return DbtCloudRunJobOperator(
        task_id = task_id,

        ## Airflow connection
        dbt_cloud_conn_id = conn_id,

        ## dbt Cloud job
        job_id = job_id,

        ## CLEAN PRODUCTION SETTINGS
        wait_for_termination = True,
        check_interval = 60,
        timeout = 600,

        ## saving workers
        deferrable = defferable,

        ## observability
        ## additional_run_config = {
        ##     "cause": "Triggered by Airflow"
        ## }
        trigger_reason = trigger_reason
    )
