"""CLI entrypoint for GoodsReceipt to StockTransfer."""
import argparse
from pathlib import Path

from modules.logger_setup import setup_logger
from modules.sync import run_sync


def _parse_args():
    parser = argparse.ArgumentParser(
        description="SAP Goods Receipt to StockTransferRequest sync"
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Run in production mode",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (no POST to Service Layer)",
    )
    parser.add_argument(
        "--date_from",
        type=str,
        default=None,
        help="Optional date override in ISO format (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date_to",
        type=str,
        default=None,
        help="Not used by logic, only logged",
    )
    return parser.parse_args()


def main() -> None:
    log_path = Path(__file__).resolve().parent / "logs" / "log.log"
    logger = setup_logger(
        log_file=str(log_path),
        integration_name="KRE GoodsReceipt to StockTransfer",
    )

    args = _parse_args()
    if args.prod and args.test:
        logger.warning("Both --prod and --test are set. Using --prod.")
        mode = "prod"
    elif args.prod:
        mode = "prod"
    else:
        mode = "test"

    logger.info(
        "Starting CLI with mode=%s, date_from=%s, date_to=%s",
        mode,
        args.date_from,
        args.date_to,
    )
    run_sync(mode=mode, date_from=args.date_from, date_to=args.date_to)


if __name__ == "__main__":
    main()
