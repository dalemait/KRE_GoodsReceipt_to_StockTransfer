# modules/api_serviceLayer.py
import logging
import requests

logger = logging.getLogger(__name__)


def login_service_layer(sl_url, sl_user, sl_password, sl_company):
    payload = {
        "UserName": sl_user,
        "Password": sl_password,
        "CompanyDB": sl_company
    }
    response = requests.post(f"{sl_url}/Login", json=payload, verify=False)
    if response.status_code != 200 or not response.cookies.get("B1SESSION"):
        logger.error("❌ Innlogging til SAP Service Layer feilet")
        raise Exception("Innlogging til Service Layer feilet")
    logger.info("🔐 Innlogging til SAP Service Layer vellykket")
    return response.cookies, response.cookies.get("B1SESSION")
