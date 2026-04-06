"""
INSAF Payroll Intelligence Platform — Full Pipeline Orchestrator

Runs the complete ETL + DW load in one command:
  1. DW1: clean paie2015.json  → fact_paie + 6 shared dimensions
  2. DW2: clean ind2015.json   → fact_indem + dim_indemnite
  3. Load all JSONL into PostgreSQL (payroll_dw)

Usage:
    python -m etl.run_pipeline
    python -m etl.run_pipeline --reset      # truncate facts before loading
    python -m etl.run_pipeline --skip-etl   # only reload DB (data/clean already fresh)

Exit codes:
    0  — success
    1  — ETL quality gate failed
    2  — DB load failed
"""
from __future__ import annotations

import argparse
import sys
import uuid

from etl.core.logger import get_logger
from etl.pipeline_paie  import run as run_paie
from etl.pipeline_indem import run as run_indem
from etl.load_dw        import run as load_dw


def main():
    parser = argparse.ArgumentParser(description="Run full INSAF ETL + DW load")
    parser.add_argument("--reset",     action="store_true",
                        help="Truncate fact tables before loading (full reload)")
    parser.add_argument("--skip-etl",  action="store_true",
                        help="Skip ETL and go straight to DB load (reuse existing JSONL)")
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:8]
    log    = get_logger("run_pipeline", run_id=run_id)

    log.info("=" * 60)
    log.info("INSAF Full Pipeline  run_id=%s", run_id)
    log.info("=" * 60)

    # ── Step 1 & 2: ETL ───────────────────────────────────────────────────────
    if not args.skip_etl:
        log.info("Step 1/3 — DW1 ETL (payroll)...")
        try:
            report_paie = run_paie(run_id=run_id)
            qg = report_paie["quality_gate"]
            if "FAIL" in qg["status"] and not qg["status"].startswith("PASS"):
                log.error("DW1 quality gate FAILED — aborting. Errors: %s", qg["errors"])
                sys.exit(1)
            log.info("Step 1 done — %d rows, status=%s",
                     report_paie["stats"]["written"], qg["status"])
        except Exception as exc:
            log.error("DW1 ETL crashed: %s", exc)
            raise

        log.info("Step 2/3 — DW2 ETL (indemnities)...")
        try:
            report_indem = run_indem(run_id=run_id)
            qg = report_indem["quality_gate"]
            if "FAIL" in qg["status"] and not qg["status"].startswith("PASS"):
                log.error("DW2 quality gate FAILED — aborting. Errors: %s", qg["errors"])
                sys.exit(1)
            log.info("Step 2 done — %d rows, status=%s",
                     report_indem["stats"]["written"], qg["status"])
        except Exception as exc:
            log.error("DW2 ETL crashed: %s", exc)
            raise
    else:
        log.info("--skip-etl set — skipping ETL, using existing JSONL files")

    # ── Step 3: DB load ───────────────────────────────────────────────────────
    log.info("Step 3/3 — Loading into PostgreSQL...")
    try:
        load_dw(reset=args.reset)
    except Exception as exc:
        log.error("DB load failed: %s", exc)
        sys.exit(2)

    log.info("=" * 60)
    log.info("Pipeline complete — both DWs ready in PostgreSQL.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
