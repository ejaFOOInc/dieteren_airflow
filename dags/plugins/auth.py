import struct
import logging
import requests
from airflow.hooks.base import BaseHook


def get_auth_token(
        connection
):
    logger = logging.getLogger("airflow.task")
    
    try:
        conn = BaseHook.get_connection(connection)
        extra = conn.extra_dejson

        TENANT_ID = extra.get("tenantID") or extra.get("tenant_id")
        CLIENT_ID = conn.login or extra.get("clientId") or extra.get("client_id")
        CLIENT_SECRET = conn.password or extra.get("clientSecret") or extra.get("client_secret")

        url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
        data = {
        	"grant_type": "client_credentials",
        	"client_id": CLIENT_ID,
        	"client_secret": CLIENT_SECRET,
        	"scope": "https://api.fabric.microsoft.com/.default"
    	}
        r = requests.post(url, data=data)
        token_str = r.json()["access_token"]
        # Pack the token into the format required by the ODBC driver
        token_bytes = token_str.encode("utf-16-le")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s",
            len(token_bytes),
            token_bytes,
        )

        logger.info("✅ Successfully retrieved Connection Token")
        logger.info(f"The full url constructed: {url}")
        logger.info(f"The data posted to the url: {data}")
        logger.info(f"Full airflow connection: {conn}")
        logger.info(f"The extracted tennantId: {TENANT_ID}")
        logger.info(f"The extracted clientId: {CLIENT_ID}")

        return token_struct
    
    except Exception as e:
        logger.error(f"❌ FAILURE to retrieve Connection Token - for connection {connection}")
        raise