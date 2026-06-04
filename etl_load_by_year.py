"""
Load full paie.json year by year to avoid disk space issues.
Deletes the JSONL after each year is loaded into the DB.
"""
import sys
from pathlib import Path

SOURCE_PAIE  = Path("data/newRawData/paie.json")
SOURCE_INDEM = Path("data/newRawData/export_indem.json")
FACT_JSONL   = Path("data/clean/fact_paie.jsonl")
YEARS        = list(range(2015, 2027))

from etl.pipeline_paie import run as run_paie
from etl.load_dw       import run as load_dw
from etl.core.logger   import get_logger
import uuid

log = get_logger("etl_load_by_year")

# First year: reset DB. Subsequent years: append.
first = True
for year in YEARS:
    log.info("=" * 50)
    log.info("Processing year %d ...", year)
    log.info("=" * 50)

    run_id = uuid.uuid4().hex[:8]

    try:
        report = run_paie(
            source=SOURCE_PAIE,
            run_id=run_id,
            year_min=year,
            year_max=year,
        )
        rows = report["stats"]["written"]
        log.info("Year %d ETL done — %d rows", year, rows)
    except Exception as e:
        log.error("Year %d ETL failed: %s", year, e)
        sys.exit(1)

    if rows == 0:
        log.info("Year %d — no rows, skipping DB load", year)
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
        first = False
        continue

    try:
        load_dw(reset=first)
        log.info("Year %d loaded into DB", year)
    except Exception as e:
        log.error("Year %d DB load failed: %s", year, e)
        sys.exit(1)

    # Free disk space before next year
    if FACT_JSONL.exists():
        FACT_JSONL.unlink()
        log.info("Deleted fact_paie.jsonl to free disk space")

    first = False

log.info("All years loaded. Now loading indemnities...")

# Load indem once at the end
from etl.pipeline_indem import run as run_indem
try:
    run_indem(source=SOURCE_INDEM, run_id=uuid.uuid4().hex[:8])
    load_dw(reset=False)
    log.info("Indemnities loaded.")
except Exception as e:
    log.error("Indem load failed: %s", e)

log.info("DONE — full dataset loaded.")
