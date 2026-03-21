import requests
import json

POWER_AUTOMATE_URL = ""
DYNATRACE_URL = ""

def pa_post(context, message):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = str(context["execution_date"])
    log_url = context["task_instance"].log_url

    payload = {
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "log_url": log_url,
        "message": message,
        'recipient': "",
        'status': ""
    }

    requests.post(
        POWER_AUTOMATE_URL,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )

def dt_post(context, status):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    token = ""
    headers = {
        "Authorization": f"Api-Token {token}",
        "Content-Type": "text/plain"
    }

    payload = f"custom.airflow.pipeline,env=dv,dag={dag_id},task={task_id},status={status} 1"

    logger = logging.getLogger("Dynatrace.post")

    logger.info(f"Dynatrace header: Authorization - Api-Token")
    logger.info(f"Dynatrace header: Content-Type - {headers['Content-Type']}")
    logger.info(f"Dynatrace payload: {payload}")

    try:
        postResult = requests.post(
            DYNATRACE_URL,
            data=payload,
            headers=headers
        )
        logger.info(f"SUCCESS - {str(postResult)}")
    except Exception as e:
        logger.error(f"ERROR - Request Post failed - {str(e)}")

def success_callback(context):
    pa_post(context, "SUCCESS")
    dt_post(context, "SUCCESS")

def failure_callback(context):
    pa_post(context, "FAILURE")
    dt_post(context, "FAILURE")
