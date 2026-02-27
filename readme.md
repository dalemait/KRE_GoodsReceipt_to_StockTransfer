# SAP GoodsReceipt to StockTransfer

Automated integration that:

1. Reads yesterday's goods receipts from SAP HANA (`OPDN/PDN1`).
2. Applies quantity factor logic:
   - If `U_TaxEU = 0` -> exclude item.
   - If `U_TaxEU > 0` -> use `U_TaxEU` factor.
   - Else -> use `U_DSS_StoreMarkup` factor.
3. Calculates transfer quantity and rounds it to whole inner/outer pack quantity.
4. Excludes rows with final quantity `<= 0`.
5. Creates new `InventoryTransferRequests` in SAP Service Layer (never PATCH existing requests).

## Structure

- `main.py` - CLI entrypoint (`--prod`, `--test`, `--date_from`, `--date_to`)
- `modules/`
  - `logger_setup.py` - logging with daily rotation + dashboard batching
  - `sap_client.py` - SAP HANA client
  - `api_serviceLayer.py` - SAP Service Layer login
  - `sync.py` - main integration flow
  - `sql/goods_receipt_qty.sql` - SQL query and quantity calculations
- `logs/log.log` - runtime log
- `.env` - configuration
- `last_sync.json` - timestamp for last run

## Usage

Test mode:

```bash
python main.py --test
```

Production mode:

```bash
python main.py --prod
```

Optional date override (instead of default yesterday):

```bash
python main.py --prod --date_from 2026-02-26
```
