"""
DW2 Pipeline — Indemnities (pa_type = "3")
═══════════════════════════════════════════
Source : data/raw/ind2015.json   (NOTE: invalid JSON — comma decimals fixed by reader)
         data/raw/indem_def.json (indemnity code reference)
Output :
  data/clean/fact_indem.jsonl        ← slim: FKs + measures + DQ flags only
  data/clean/dim_indemnite.jsonl     (from indem_def.json)
  reports/pipeline_indem_<run_id>.json

Shared dimensions (employee, grade, nature, organisme, region, time) are
written by pipeline_paie.py and reused here — they are not re-generated.

Run:
  python -m etl.pipeline_indem
  python -m etl.pipeline_indem --source data/raw/ind2015.json
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from etl.core.config import (
    CLEAN_DIR, CLEAN_FACT_INDEM, CLEAN_DIM_INDEMNITE,
    INDEM_TYPE_FILTER, REPORTS_DIR,
    RAW_GRADE, RAW_INDEM, RAW_INDEM_DEF, RAW_NATURE, RAW_ORGANISME, RAW_REGION,
    QG_GRADE_MIN_MATCH, QG_NATURE_MIN_MATCH,
    QG_ORGANISME_WARN_AT, QG_REGION_WARN_AT,
)
from etl.core.logger import get_logger
from etl.ingestion.readers import stream_records
from etl.cleaning.encoding import fix_record
from etl.cleaning.normalizer import normalize_payroll_record
from etl.mapping import grade as grade_map
from etl.mapping import nature as nature_map
from etl.mapping import organisme as org_map
from etl.mapping import region as region_map
from etl.mapping import indemnite as indem_map


def run(source: Path = RAW_INDEM, run_id: str | None = None) -> dict:
    run_id  = run_id or uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc)
    log     = get_logger("pipeline_indem", run_id=run_id)

    log.info("DW2 pipeline started — source=%s", source.name)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load reference lookups ────────────────────────────────────────────────
    log.info("Loading reference data...")
    grade_lookup   = grade_map.build_lookup(RAW_GRADE)
    nature_lookup  = nature_map.build_lookup(RAW_NATURE)
    org_lookup     = org_map.build_lookup(RAW_ORGANISME)
    region_lookup  = region_map.build_lookup(RAW_REGION)
    indem_lookup   = indem_map.build_lookup(RAW_INDEM_DEF)

    log.info("Lookups — grade=%d  nature=%d  org=%d  region=%d  indemnite=%d",
             len(grade_lookup), len(nature_lookup),
             len(org_lookup["full"]), len(region_lookup["by_dep"]),
             len(indem_lookup))

    # ── Write dim_indemnite ───────────────────────────────────────────────────
    _write_jsonl(CLEAN_DIM_INDEMNITE, indem_lookup.values())
    log.info("dim_indemnite written — %d codes", len(indem_lookup))

    # ── Stats ─────────────────────────────────────────────────────────────────
    stats = {
        "total_raw": 0, "skipped_type": 0, "written": 0,
        "grade_matched": 0, "nature_matched": 0,
        "org_matched": 0,   "region_matched": 0,
        "has_issues": 0,
    }

    # ── Stream + clean + map ──────────────────────────────────────────────────
    with open(CLEAN_FACT_INDEM, "w", encoding="utf-8") as fout:
        for raw in stream_records(source):
            stats["total_raw"] += 1

            # Filter on pa_type
            if str(raw.get("pa_type") or raw.get("PA_TYPE") or "").strip() != INDEM_TYPE_FILTER:
                stats["skipped_type"] += 1
                continue

            # Encoding fix (Arabic labels in person names, org codes, etc.)
            raw = fix_record(raw)

            # Field normalization (same rules as paie — same schema)
            rec, issues = normalize_payroll_record(raw)
            if issues:
                stats["has_issues"] += 1

            # Reference matching (same shared dimensions)
            grade,  gm = grade_map.match(rec.get("pa_grd"),  grade_lookup)
            nature, nm = nature_map.match(rec.get("pa_natu"), nature_lookup)
            org,    om = org_map.match(rec, org_lookup)
            rgn,    rm = region_map.match(rec, region_lookup)

            if grade:  stats["grade_matched"]  += 1
            if nature: stats["nature_matched"] += 1
            if org:    stats["org_matched"]    += 1
            if rgn:    stats["region_matched"] += 1

            # Collect time periods (used for dim_time — written by pipeline_paie,
            # but we log for completeness)
            yr = rec.get("pa_annee")
            mo = rec.get("pa_mois")
            mat = rec.get("pa_mat")

            # ── Build slim fact row ───────────────────────────────────────────
            fact_row = {
                # Natural foreign keys
                "employee_id":  mat,
                "year_num":     int(yr) if yr else None,
                "month_num":    int(mo) if mo else None,
                "grade_code":   rec.get("pa_grd"),
                "nature_code":  rec.get("pa_natu"),
                "org_codetab":  org["codetab"] if org else None,
                "org_dire":     org["dire"]    if org else None,
                "pa_loca_raw":  rec.get("pa_loca"),
                # Degenerate dimensions
                "pa_type":      rec.get("pa_type"),
                "pa_sec":       rec.get("pa_sec"),
                "pa_eche":      rec.get("pa_eche"),
                "pa_indice":    rec.get("pa_indice"),
                "pa_sitfam":    rec.get("pa_sitfam"),
                "pa_nbrfam":    rec.get("pa_nbrfam"),
                "pa_enfits":    rec.get("pa_enfits"),
                "pa_totinf":    rec.get("pa_totinf"),
                "pa_article":   rec.get("pa_article"),
                "pa_parag":     rec.get("pa_parag"),
                "pa_mp":        rec.get("pa_mp"),
                "pa_regcnr":    rec.get("pa_regcnr"),
                # Measures
                "m_salnimp":    rec.get("pa_salnimp"),
                "m_avkm":       rec.get("pa_avkm"),
                "m_rapni":      rec.get("pa_rapni"),
                "m_salimp":     rec.get("pa_salimp"),
                "m_salbrut":    rec.get("pa_salbrut"),
                "m_netord":     rec.get("pa_netord"),
                "m_brutcnr":    rec.get("pa_brutcnr"),
                "m_sps":        rec.get("pa_sps"),
                "m_rapsalb":    rec.get("pa_rapsalb"),
                "m_spl":        rec.get("pa_spl"),
                "m_cps":        rec.get("pa_cps"),
                "m_avlog":      rec.get("pa_avlog"),
                "m_netpay":     rec.get("pa_netpay"),
                "m_retrait":    rec.get("pa_retrait"),
                "m_sub":        rec.get("pa_sub"),
                "m_cpe":        rec.get("pa_cpe"),
                "m_rapimp":     rec.get("pa_rapimp"),
                "m_capdeces":   rec.get("pa_capdeces"),
                # DQ flags
                "dq_has_issues":     rec.get("dq_has_issues"),
                "dq_issue_count":    rec.get("dq_issue_count"),
                "dq_grade_matched":  grade  is not None,
                "dq_nature_matched": nature is not None,
                "dq_org_matched":    org    is not None,
                "dq_region_matched": rgn    is not None,
                "dq_grade_method":   gm,
                "dq_nature_method":  nm,
                "dq_org_method":     om,
                "dq_region_method":  rm,
                # Audit
                "run_id":      run_id,
                "source_file": source.name,
            }

            fout.write(json.dumps(fact_row, ensure_ascii=False, default=str) + "\n")
            stats["written"] += 1

            if stats["written"] % 20_000 == 0:
                log.info("  %d rows written...", stats["written"])

    log.info("Fact file written — %d rows", stats["written"])

    # ── Quality gate ──────────────────────────────────────────────────────────
    n = stats["written"] or 1
    pcts = {
        "pct_grade_matched":  round(100 * stats["grade_matched"]  / n, 2),
        "pct_nature_matched": round(100 * stats["nature_matched"] / n, 2),
        "pct_org_matched":    round(100 * stats["org_matched"]    / n, 2),
        "pct_region_matched": round(100 * stats["region_matched"] / n, 2),
        "pct_has_issues":     round(100 * stats["has_issues"]     / n, 2),
    }

    qg_pass   = True
    qg_errors = []
    qg_warns  = []

    if pcts["pct_grade_matched"]  / 100 < QG_GRADE_MIN_MATCH:
        qg_pass = False
        qg_errors.append(f"grade match {pcts['pct_grade_matched']}% < threshold")
    if pcts["pct_nature_matched"] / 100 < QG_NATURE_MIN_MATCH:
        qg_pass = False
        qg_errors.append(f"nature match {pcts['pct_nature_matched']}% < threshold")
    if pcts["pct_org_matched"]    / 100 < QG_ORGANISME_WARN_AT:
        qg_warns.append(f"org match {pcts['pct_org_matched']}% (known limitation)")
    if pcts["pct_region_matched"] / 100 < QG_REGION_WARN_AT:
        qg_warns.append(f"region match {pcts['pct_region_matched']}% — pa_loca unmappable")

    status = "PASS" if qg_pass else "FAIL"
    if qg_warns:
        status += "_WITH_WARNINGS"

    if not qg_pass:
        log.error("Quality gate FAILED: %s", qg_errors)
    for w in qg_warns:
        log.warning("QG warning: %s", w)
    log.info("Quality gate: %s", status)

    # ── Write report ──────────────────────────────────────────────────────────
    report = {
        "run_id":       run_id,
        "pipeline":     "DW2_indem",
        "started_at":   started.isoformat(),
        "finished_at":  datetime.now(timezone.utc).isoformat(),
        "source":       str(source),
        "stats":        {**stats, **pcts},
        "quality_gate": {"status": status, "errors": qg_errors, "warnings": qg_warns},
        "outputs": {
            "fact_indem":    str(CLEAN_FACT_INDEM),
            "dim_indemnite": str(CLEAN_DIM_INDEMNITE),
        },
    }

    report_path = REPORTS_DIR / f"pipeline_indem_{run_id}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Report written: %s", report_path.name)

    return report


def _write_jsonl(path: Path, records) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(dict(rec), ensure_ascii=False, default=str) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DW2 (indemnities) ETL pipeline")
    parser.add_argument("--source", type=Path, default=RAW_INDEM)
    args = parser.parse_args()
    run(source=args.source)
