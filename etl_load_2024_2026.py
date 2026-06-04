"""
Load only 2024, 2025, 2026 one year at a time (2015-2023 already in DB).
Keeps each JSONL to ~1.2 GB to avoid OOM on PostgreSQL COPY.
"""
import sys
from pathlib import Path
import uuid

SOURCE_PAIE = Path("data/newRawData/paie.json")
FACT_JSONL  = Path("data/clean/fact_paie.jsonl")

from etl.pipeline_paie import run as run_paie
from etl.load_dw       import run as load_dw
from etl.core.logger   import get_logger

log = get_logger("etl_load_2024_2026")

for year in [2024, 2025, 2026]:
    log.info("=" * 50)
    log.info("Processing year %d ...", year)
    log.info("=" * 50)

    # Clean up any leftover JSONL from a previous crash
    if FACT_JSONL.exists():
        FACT_JSONL.unlink()
        log.info("Removed stale fact_paie.jsonl")

    run_id = uuid.uuid4().hex[:8]

    try:
        report = run_paie(source=SOURCE_PAIE, run_id=run_id, year_min=year, year_max=year)
        rows = report["stats"]["written"]
        log.info("Year %d ETL done — %d rows", year, rows)
    except Exception as e:
        log.error("Year %d ETL failed: %s", year, e)
        sys.exit(1)

    if rows == 0:
        log.info("Year %d — no rows found, skipping", year)
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
        continue

    try:
        load_dw(reset=False)
        log.info("Year %d loaded into DB", year)
    except Exception as e:
        log.error("Year %d DB load failed: %s", year, e)
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
        sys.exit(1)

    if FACT_JSONL.exists():
        FACT_JSONL.unlink()
        log.info("Deleted fact_paie.jsonl to free disk space")

log.info("DONE — 2024-2026 loaded. Indemnities already present, skipping.")
