# modules/sap_client.py
import logging
import os
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class SAPClient:
    """
    Enkel HANA-klient som kjører SQL fra modules/sql/<filnavn>.sql
    Krever hdbcli (SAP HANA client + pip-pakken 'hdbcli').
    """

    def __init__(self, url: str, username: str, password: str):
        self.url = url
        self.username = username
        self.password = password

    def _connect(self):
        from hdbcli import dbapi

        if ":" in self.url:
            host, port = self.url.split(":")
            port = int(port)
        else:
            host, port = self.url, 30015

        return dbapi.connect(
            address=host,
            port=port,
            user=self.username,
            password=self.password,
        )
