"""Sync Goods Receipts to new Inventory Transfer Requests."""
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .api_serviceLayer import login_service_layer
from .sap_client import SAPClient

logger = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo

    OSLO_TZ = ZoneInfo("Europe/Oslo")
except Exception:
    OSLO_TZ = None

REQUEST_COMMENT = "dalema_integration"
SQL_FILE_NAME = "goods_receipt_qty.sql"


def _now_oslo() -> datetime:
    if OSLO_TZ:
        return datetime.now(OSLO_TZ)
    return datetime.now()


def _load_env() -> Dict[str, Any]:
    load_dotenv()
    cfg = {
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        "LAST_SYNC_FILE": os.getenv("LAST_SYNC_FILE", "last_sync.json"),
        "FROM_WAREHOUSE": os.getenv("FROM_WAREHOUSE", "999"),
        "TO_WAREHOUSE": os.getenv("TO_WAREHOUSE", "221"),
        "HANA_URL": os.getenv("HANA_URL", ""),
        "HANA_USER": os.getenv("HANA_USER", ""),
        "HANA_PASSWORD": os.getenv("HANA_PASSWORD", ""),
        "SL_URL": os.getenv("SL_URL", ""),
        "SL_USER": os.getenv("SL_USER", ""),
        "SL_PASSWORD": os.getenv("SL_PASSWORD", ""),
        "SL_COMPANY": os.getenv("SL_COMPANY", ""),
    }
    logger.debug(
        "Configuration loaded from env: %s",
        {k: v for k, v in cfg.items() if "PASS" not in k},
    )
    return cfg


def _read_last_sync(path: str) -> Optional[datetime]:
    if not os.path.exists(path):
        logger.info("No last_sync file found (%s).", path)
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        value = data.get("last_sync")
        if not value:
            return None
        return datetime.fromisoformat(value)
    except Exception as exc:
        logger.warning("Could not parse last_sync file (%s): %s", path, exc)
        return None


def _write_last_sync(path: str) -> None:
    now = _now_oslo()
    value = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    payload = {"last_sync": value}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    logger.info("Updated last_sync to %s", value)


def _resolve_target_date(date_from: Optional[str]) -> datetime:
    if date_from:
        try:
            return datetime.fromisoformat(date_from)
        except ValueError:
            logger.warning(
                "Invalid date_from '%s'. Falling back to yesterday in Europe/Oslo.",
                date_from,
            )

    return _now_oslo() - timedelta(days=1)


def _render_sql(sql_template: str, cfg: Dict[str, Any], target_date: datetime) -> str:
    rendered = sql_template
    rendered = rendered.replace("{{TARGET_DATE}}", target_date.strftime("%Y-%m-%d"))
    rendered = rendered.replace("{{FROM_WAREHOUSE}}", cfg["FROM_WAREHOUSE"])
    rendered = rendered.replace("{{TO_WAREHOUSE}}", cfg["TO_WAREHOUSE"])
    return rendered


def _query_sap_goods_receipts(
    cfg: Dict[str, Any],
    target_date: datetime,
) -> List[Dict[str, Any]]:
    sql_path = Path(__file__).resolve().parent / "sql" / SQL_FILE_NAME
    logger.info("Running SAP query from %s for date %s", sql_path, target_date.date())

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_template = f.read()

    sql = _render_sql(sql_template, cfg, target_date)

    client = SAPClient(
        url=cfg["HANA_URL"],
        username=cfg["HANA_USER"],
        password=cfg["HANA_PASSWORD"],
    )

    conn = client._connect()
    cur = conn.cursor()

    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    items: List[Dict[str, Any]] = []
    for row in rows:
        qty = float(row[1] or 0)
        if qty <= 0:
            continue

        item = {
            "ItemCode": row[0],
            "Quantity": qty,
            "FromWarehouseCode": row[2],
            "WarehouseCode": row[3],
            "ReceivedQty": float(row[4] or 0) if len(row) > 4 else None,
            "FactorUsed": float(row[5] or 0) if len(row) > 5 else None,
        }
        items.append(item)

    logger.info("SAP query returned %s transfer row(s)", len(items))
    return items


def _sort_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            (item.get("WarehouseCode") or "").upper(),
            (item.get("ItemCode") or "").upper(),
        ),
    )


def _build_lines(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lines: List[Dict[str, Any]] = []
    line_num = 0
    for item in items:
        qty = float(item.get("Quantity", 0) or 0)
        if qty <= 0:
            continue

        lines.append(
            {
                "LineNum": line_num,
                "ItemCode": item["ItemCode"],
                "Quantity": qty,
                "FromWarehouseCode": item["FromWarehouseCode"],
                "WarehouseCode": item["WarehouseCode"],
            }
        )
        line_num += 1

    return lines


def _create_requests(cfg: Dict[str, Any], cookies, items: List[Dict[str, Any]]) -> None:
    url = cfg["SL_URL"].rstrip("/") + "/InventoryTransferRequests"
    lines = _build_lines(items)
    if not lines:
        logger.info("No valid lines to create.")
        return

    payload = {
        "Comments": REQUEST_COMMENT,
        "FromWarehouse": cfg["FROM_WAREHOUSE"],
        "ToWarehouse": cfg["TO_WAREHOUSE"],
        "StockTransferLines": lines,
    }

    logger.info(
        "Creating one InventoryTransferRequest with %s line(s)",
        len(lines),
    )
    response = requests.post(url, json=payload, cookies=cookies, verify=False)
    if not response.ok:
        logger.error(
            "POST InventoryTransferRequests failed: %s - %s",
            response.status_code,
            response.text,
        )
        raise Exception("POST InventoryTransferRequests failed")

    logger.info("InventoryTransferRequest created successfully.")


def _simulate_requests(cfg: Dict[str, Any], items: List[Dict[str, Any]]) -> None:
    lines = _build_lines(items)
    logger.info(
        "[TEST] Would create one InventoryTransferRequest with %s line(s), from %s to %s",
        len(lines),
        cfg["FROM_WAREHOUSE"],
        cfg["TO_WAREHOUSE"],
    )


def run_sync(
    mode: str = "prod",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> None:
    cfg = _load_env()
    logger.info(
        "Starting GoodsReceipt to StockTransfer sync in mode '%s' (date_to=%s)",
        mode,
        date_to,
    )

    last_sync = _read_last_sync(cfg["LAST_SYNC_FILE"])
    if last_sync:
        logger.debug("Last sync was %s", last_sync)

    target_date = _resolve_target_date(date_from)
    logger.info("Goods receipt source date is %s", target_date.date())

    items = _query_sap_goods_receipts(cfg, target_date)
    if not items:
        logger.info("No rows to transfer. Nothing to create.")
        if mode == "prod":
            _write_last_sync(cfg["LAST_SYNC_FILE"])
        return

    items = _sort_items(items)

    if mode == "test":
        _simulate_requests(cfg, items)
        return

    cookies, session = login_service_layer(
        cfg["SL_URL"],
        cfg["SL_USER"],
        cfg["SL_PASSWORD"],
        cfg["SL_COMPANY"],
    )
    logger.debug("Service Layer session: %s", session)

    _create_requests(cfg, cookies, items)
    _write_last_sync(cfg["LAST_SYNC_FILE"])

    logger.info("Job completed (prod mode).")
