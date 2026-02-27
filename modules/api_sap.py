# modules/api_sap.py
import logging
import requests

logger = logging.getLogger(__name__)


def login_sap_api(sl_url, sl_user, sl_password, sl_company):
    payload = {
        "UserName": sap_api_user,
        "Password": sap_api_password,
        "CompanyDB": sap_api_company
    }
    response = requests.post(f"{sl_url}/Login", json=payload, verify=False)
    if response.status_code != 200 or not response.cookies.get("B1SESSION"):
        logger.error("❌ Innlogging til SAP-API feilet")
        raise Exception("Innlogging til SAP-API feilet")
    logger.info("🔐 Innlogging til SAP-API vellykket")
    return response.cookies, response.cookies.get("B1SESSION")
