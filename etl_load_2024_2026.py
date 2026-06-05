"""
Load only 2024, 2025, 2026 one year at a time (2015-2023 already in DB).
Keeps each JSONL to ~1.2 GB to avoid OOM on PostgreSQL COPY.
"""
import shutil
import sys
from pathlib import Path
import uuid

SOURCE_PAIE    = Path("data/newRawData/paie.json")
FACT_JSONL     = Path("data/clean/fact_paie.jsonl")
MIN_FREE_GB    = 8          # abort if C: drops below this
WARN_FREE_GB   = 12         # warn when getting close

from etl.pipeline_paie import run as run_paie
from etl.load_dw       import run as load_dw
from etl.core.logger   import get_logger

log = get_logger("etl_load_2024_2026")


def free_gb() -> float:
    return shutil.disk_usage("C:\\").free / 1024 ** 3


def check_disk(stage: str) -> None:
    gb = free_gb()
    if gb < MIN_FREE_GB:
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
            log.warning("Deleted partial JSONL to recover space")
        log.error("ABORT at [%s] — only %.1f GB free (minimum %d GB required). "
                  "Free up disk space and re-run.", stage, gb, MIN_FREE_GB)
        sys.exit(1)
    if gb < WARN_FREE_GB:
        log.warning("[%s] Low disk space: %.1f GB free", stage, gb)


for year in [2024, 2025, 2026]:
    log.info("=" * 50)
    log.info("Processing year %d  |  %.1f GB free", year, free_gb())
    log.info("=" * 50)

    check_disk(f"start-{year}")

    # Clean up any leftover JSONL from a previous crash
    if FACT_JSONL.exists():
        FACT_JSONL.unlink()
        log.info("Removed stale fact_paie.jsonl")

    run_id = uuid.uuid4().hex[:8]

    try:
        report = run_paie(source=SOURCE_PAIE, run_id=run_id, year_min=year, year_max=year)
        rows = report["stats"]["written"]
        log.info("Year %d ETL done — %d rows  |  %.1f GB free", year, rows, free_gb())
    except Exception as e:
        log.error("Year %d ETL failed: %s", year, e)
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
        sys.exit(1)

    if rows == 0:
        log.info("Year %d — no rows found, skipping", year)
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
        continue

    check_disk(f"before-load-{year}")

    try:
        load_dw(reset=False)
        log.info("Year %d loaded into DB  |  %.1f GB free", year, free_gb())
    except Exception as e:
        log.error("Year %d DB load failed: %s", year, e)
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
        sys.exit(1)

    if FACT_JSONL.exists():
        FACT_JSONL.unlink()
        log.info("Deleted fact_paie.jsonl  |  %.1f GB free", free_gb())

log.info("DONE — 2024-2026 loaded. Indemnities already present, skipping.")
