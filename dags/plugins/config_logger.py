import logging


def config_logger(
        environment :str
):
    
    logger = logging.getLogger("airflow.task")
    logger.info(f"environment: {environment}")