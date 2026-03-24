from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricPipelineJobParameters

import logging

def fabric_run_pipeline(
        task_id :str,
        fabric_conn_id :str,
        workspace_id :str,
        item_id :str,
        timeout :int,
        deferrable :bool,
        parm_SourceName :str,
        parm_ApplicationName :str
):
    
    logger = logging.getLogger("airflow.task")
    logger.info(f"Use deferred for this task: {deferrable}")
    
    return MSFabricRunJobOperator(
        task_id=task_id,
        fabric_conn_id=fabric_conn_id,
        workspace_id=workspace_id,
        item_id=item_id,
        job_type="Pipeline",
        timeout=timeout,
        deferrable=deferrable,
        job_params=MSFabricPipelineJobParameters()
            .set_parameter("SourceName", parm_SourceName)
            .set_parameter("ApplicationName", parm_ApplicationName)
            .set_parameter("ObjectName", "[object Object]")
            .to_json()
    )