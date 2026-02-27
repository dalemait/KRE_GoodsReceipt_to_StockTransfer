import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path
import requests
import threading
import time
import uuid
import atexit
from threading import Thread

from dotenv import load_dotenv


class DashboardHandler(logging.Handler):
    """Handler that sends logs to Dalema Monitoring in batches."""

    def __init__(
        self,
        endpoint: str,
        integration_name: str,
        batch_size: int = 50,
        flush_interval: float = 2.0,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.integration_name = integration_name
        self.run_id = str(uuid.uuid4())
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.last_flush = time.time()
        self._flush_thread = None
        self._running = True
        self._start_flush_timer()

    def emit(self, record):
        log_entry = {
            "integration_name": self.integration_name,
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            "run_id": self.run_id,
            "timestamp": int(time.time() * 1000),
        }
        with self.buffer_lock:
            self.buffer.append(log_entry)
            if len(self.buffer) >= self.batch_size:
                self._flush()

    def _start_flush_timer(self):
        def timer_loop():
            while self._running:
                time.sleep(0.5)
                if time.time() - self.last_flush >= self.flush_interval:
                    self.flush()

        self._flush_thread = Thread(target=timer_loop, daemon=True)
        self._flush_thread.start()

    def flush(self):
        with self.buffer_lock:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        logs_to_send = self.buffer.copy()
        self.buffer = []
        self.last_flush = time.time()
        Thread(target=self._send_batch, args=(logs_to_send,)).start()

    def _send_batch(self, logs):
        try:
            api_key = os.getenv("LOG_RECEIVER_API_KEY", "")
            headers = {"x-api-key": api_key} if api_key else {}
            requests.post(self.endpoint, json=logs, headers=headers, timeout=10)
        except Exception:
            pass

    def close(self):
        self._running = False
        with self.buffer_lock:
            logs_to_send = self.buffer.copy()
            self.buffer = []
        if logs_to_send:
            self._send_batch(logs_to_send)
        super().close()


def setup_logger(log_file: str = "app.log", integration_name: str = None) -> logging.Logger:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    logger = logging.getLogger()

    log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
    logger.setLevel(getattr(logging, log_level, logging.DEBUG))
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    log_dir = os.path.dirname(log_file) or "."
    os.makedirs(log_dir, exist_ok=True)
    log_file_abs = os.path.abspath(log_file)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    has_file = False
    has_stream = False
    for handler in list(logger.handlers):
        if isinstance(handler, DashboardHandler):
            logger.removeHandler(handler)
            continue
        if isinstance(handler, logging.FileHandler):
            handler_file = os.path.abspath(getattr(handler, "baseFilename", ""))
            if isinstance(handler, TimedRotatingFileHandler) and handler_file == log_file_abs:
                handler.setFormatter(fmt)
                has_file = True
            else:
                logger.removeHandler(handler)
            continue
        if isinstance(handler, logging.StreamHandler):
            has_stream = True

    if not has_file:
        fh = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            backupCount=14,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    if not has_stream:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    if integration_name:
        endpoint = os.getenv(
            "LOG_RECEIVER_ENDPOINT",
            "https://vuoaqongkzlzhowuajvl.supabase.co/functions/v1/log-receiver",
        )
        dh = DashboardHandler(endpoint, integration_name)
        dh.setFormatter(fmt)
        dh.setLevel(logging.INFO)
        logger.addHandler(dh)
        atexit.register(dh.close)

    return logger
