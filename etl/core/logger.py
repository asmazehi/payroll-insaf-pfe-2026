"""
Structured logger for the INSAF ETL pipeline.
Each pipeline run gets a unique run_id; all log entries carry it.
"""
import logging
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON — easy to ingest in any log tool."""
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts":      datetime.now(timezone.utc).isoformat(),
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        if hasattr(record, "run_id"):
            entry["run_id"] = record.run_id
        return json.dumps(entry, ensure_ascii=False)


def get_logger(name: str, run_id: str = None, log_file: Path = None) -> logging.Logger:
    """
    Return a logger for *name*.
    If run_id is provided it is injected into every record via a filter.
    If log_file is provided, a file handler is added alongside stdout.
    """
    logger = logging.getLogger(name)
    if logger.handlers:        # already configured
        return logger

    logger.setLevel(logging.DEBUG)

    # ── stdout handler ────────────────────────────────────────────────────────
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(JsonFormatter())
    logger.addHandler(sh)

    # ── optional file handler ─────────────────────────────────────────────────
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(JsonFormatter())
        logger.addHandler(fh)

    # ── inject run_id into every record ──────────────────────────────────────
    if run_id:
        class RunIdFilter(logging.Filter):
            def filter(self, record):
                record.run_id = run_id
                return True
        logger.addFilter(RunIdFilter())

    return logger
