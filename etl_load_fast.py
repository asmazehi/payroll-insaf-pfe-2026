"""
Faster ETL loader: processes 2 years per file pass instead of 1.
Each pass reads paie.json once and writes 2 years worth of JSONL (~2.4 GB),
reducing the number of 54 GB reads from 7 to 4 for years 2020-2026.

Run AFTER etl_load_by_year.py has finished year 2019.
"""
import sys
from pathlib import Path

SOURCE_PAIE  = Path("data/newRawData/paie.json")
SOURCE_INDEM = Path("data/newRawData/export_indem.json")
FACT_JSONL   = Path("data/clean/fact_paie.jsonl")

from etl.pipeline_paie import run as run_paie
from etl.load_dw       import run as load_dw
from etl.core.logger   import get_logger
import uuid, shutil

log = get_logger("etl_load_fast")

# Years already loaded: 2015-2019 (handled by etl_load_by_year.py)
# Process remaining years in 2-year batches to reduce file reads
BATCHES = [
    (2020, 2021),
    (2022, 2023),
    (2024, 2026),  # 2026 has very few rows, bundle with 2024-2025
]

for (year_min, year_max) in BATCHES:
    log.info("=" * 60)
    log.info("Processing years %d-%d ...", year_min, year_max)
    log.info("=" * 60)

    # Check free disk space — need at least 2.8 GB
    free = shutil.disk_usage("data/clean").free
    log.info("Free disk: %.1f GB", free / 1e9)
    if free < 2_800_000_000:
        log.error("Not enough disk space (%.1f GB free, need 2.8 GB)", free / 1e9)
        sys.exit(1)

    run_id = uuid.uuid4().hex[:8]

    try:
        report = run_paie(
            source=SOURCE_PAIE,
            run_id=run_id,
            year_min=year_min,
            year_max=year_max,
        )
        rows = report["stats"]["written"]
        log.info("Years %d-%d ETL done — %d rows", year_min, year_max, rows)
    except Exception as e:
        log.error("ETL failed for %d-%d: %s", year_min, year_max, e)
        sys.exit(1)

    if rows == 0:
        log.info("No rows for %d-%d, skipping load", year_min, year_max)
        if FACT_JSONL.exists():
            FACT_JSONL.unlink()
        continue

    try:
        load_dw(reset=False)
        log.info("Years %d-%d loaded into DB", year_min, year_max)
    except Exception as e:
        log.error("DB load failed for %d-%d: %s", year_min, year_max, e)
        sys.exit(1)

    if FACT_JSONL.exists():
        FACT_JSONL.unlink()
        log.info("Deleted JSONL to free disk space")

log.info("All years 2020-2026 loaded. Now loading indemnities...")

from etl.pipeline_indem import run as run_indem
try:
    run_indem(source=SOURCE_INDEM, run_id=uuid.uuid4().hex[:8])
    load_dw(reset=False)
    log.info("Indemnities loaded.")
except Exception as e:
    log.error("Indem load failed: %s", e)

log.info("DONE.")
