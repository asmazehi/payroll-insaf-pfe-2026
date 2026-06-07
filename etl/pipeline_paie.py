"""
DW1 Pipeline — Payroll (pa_type = "1")
═══════════════════════════════════════
Source : any file in data/raw/ (JSON / JSONL / CSV / Excel)
Output :
  data/clean/fact_paie.jsonl        ← slim: FKs + measures + DQ flags
  data/clean/dim_employee.jsonl
  data/clean/dim_grade.jsonl
  data/clean/dim_nature.jsonl
  data/clean/dim_organisme.jsonl
  data/clean/dim_region.jsonl
  data/clean/dim_time.jsonl
  reports/pipeline_paie_<run_id>.json

Run:
  python -m etl.pipeline_paie
  python -m etl.pipeline_paie --source data/raw/paie2015.json
"""
from __future__ import annotations

import argparse
import json
import uuid
from calendar import monthrange
from datetime import datetime, timezone
from typing import Callable
from pathlib import Path

from etl.core.config import (
    CLEAN_DIR, CLEAN_DIM_EMPLOYEE, CLEAN_DIM_GRADE, CLEAN_DIM_NATURE,
    CLEAN_DIM_ORGANISME, CLEAN_DIM_REGION, CLEAN_DIM_TIME,
    CLEAN_FACT_PAIE, PAIE_TYPE_FILTER, REPORTS_DIR,
    RAW_GRADE, RAW_NATURE, RAW_ORGANISME, RAW_PAIE, RAW_REGION,
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


def run(source: Path = RAW_PAIE, run_id: str | None = None,
        out_dir: Path | None = None,
        progress_cb: "Callable[[int,str],None] | None" = None,
        limit: int | None = None,
        year_min: int | None = None,
        year_max: int | None = None) -> dict:
    run_id  = run_id or uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc)
    log     = get_logger("pipeline_paie", run_id=run_id)

    _out = out_dir or CLEAN_DIR
    _fact_paie     = _out / "fact_paie.jsonl"
    _dim_employee  = _out / "dim_employee.jsonl"
    _dim_grade     = _out / "dim_grade.jsonl"
    _dim_nature    = _out / "dim_nature.jsonl"
    _dim_organisme = _out / "dim_organisme.jsonl"
    _dim_region    = _out / "dim_region.jsonl"
    _dim_time      = _out / "dim_time.jsonl"

    year_label = f"  year_filter={year_min or '*'}–{year_max or '*'}" if (year_min or year_max) else ""
    log.info("DW1 pipeline started — source=%s  out=%s%s", source.name, _out, year_label)
    _out.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    _cb = progress_cb or (lambda pct, msg, **kw: None)

    # ── Load reference lookups ────────────────────────────────────────────────
    log.info("Loading reference data...")
    _cb(15, "Loading reference lookups…")
    grade_lookup  = grade_map.build_lookup(RAW_GRADE)
    nature_lookup = nature_map.build_lookup(RAW_NATURE)
    org_lookup    = org_map.build_lookup(RAW_ORGANISME)
    region_lookup = region_map.build_lookup(RAW_REGION)

    log.info("Lookups — grade=%d  nature=%d  organisme=%d  region=%d",
             len(grade_lookup), len(nature_lookup),
             len(org_lookup["full"]), len(region_lookup["by_dep"]))
    _cb(18, f"Reference data loaded — {len(grade_lookup)} grades, {len(nature_lookup)} natures")

    # ── Accumulators ─────────────────────────────────────────────────────────
    # Employees: keep the record with the most recent (year, month) for
    # up-to-date attributes (name spelling, dates) without losing history.
    employees:    dict[str, dict]         = {}
    employee_ts:  dict[str, tuple]        = {}   # mat → (year, month)
    time_periods: set[tuple[int, int]]    = set()

    # Deduplication: natural key = (employee_id, year_num, month_num, pa_type)
    # Track within this file — DB handles cross-run deduplication via ON CONFLICT.
    seen_keys:    set[tuple]              = set()

    stats = {
        "total_raw": 0, "skipped_type": 0, "written": 0,
        "duplicates_skipped": 0,
        "grade_matched": 0, "nature_matched": 0,
        "org_matched": 0,   "region_matched": 0,
        "has_issues": 0,
    }

    # ── Stream + clean + map ──────────────────────────────────────────────────
    _SCAN_REPORT_EVERY = 500_000   # emit a progress ping every N raw records
    with open(_fact_paie, "w", encoding="utf-8") as fout:
        for raw in stream_records(source, year_min=year_min):
            stats["total_raw"] += 1

            # Periodic scan progress — keeps the UI alive during the pre-year scan
            if stats["total_raw"] % _SCAN_REPORT_EVERY == 0 and progress_cb:
                written = stats.get("written", 0)
                scanned_m = stats["total_raw"] // 1_000_000
                progress_cb(18, f"Scanning… {scanned_m}M records read, {written:,} matching so far")

            # Filter on pa_type
            if str(raw.get("pa_type") or raw.get("PA_TYPE") or "").strip() != PAIE_TYPE_FILTER:
                stats["skipped_type"] += 1
                continue

            # Optional year filter (for targeted re-runs)
            if year_min or year_max:
                try:
                    yr_raw = int(float(str(raw.get("pa_annee") or raw.get("PA_ANNEE") or 0)))
                except (TypeError, ValueError):
                    yr_raw = 0
                if year_min and yr_raw < year_min:
                    continue
                if year_max and yr_raw > year_max:
                    continue

            raw = fix_record(raw)
            rec, issues = normalize_payroll_record(raw)
            if issues:
                stats["has_issues"] += 1

            mat = rec.get("pa_mat")
            yr  = rec.get("pa_annee")
            mo  = rec.get("pa_mois")

            # ── Within-file deduplication ─────────────────────────────────────
            nat_key = (mat, yr, mo, rec.get("pa_type"))
            if nat_key in seen_keys:
                stats["duplicates_skipped"] += 1
                log.debug("Duplicate skipped: employee=%s year=%s month=%s", mat, yr, mo)
                continue
            seen_keys.add(nat_key)

            # ── Reference matching ────────────────────────────────────────────
            grade,  gm = grade_map.match(rec.get("pa_grd"),  grade_lookup)
            nature, nm = nature_map.match(rec.get("pa_natu"), nature_lookup)
            org,    om = org_map.match(rec, org_lookup)
            rgn,    rm = region_map.match(rec, region_lookup)

            if grade:  stats["grade_matched"]  += 1
            if nature: stats["nature_matched"] += 1
            if org:    stats["org_matched"]    += 1
            if rgn:    stats["region_matched"] += 1

            # ── Employee dimension (keep most recent attributes) ───────────────
            if mat:
                rec_ts = (int(yr) if yr else 0, int(mo) if mo else 0)
                if mat not in employees or rec_ts > employee_ts.get(mat, (0, 0)):
                    employees[mat] = {
                        "employee_id":      mat,
                        "last_name":        rec.get("pa_noml"),
                        "first_name":       rec.get("pa_prenl"),
                        "gender":           rec.get("pa_sexe"),
                        "birth_date":       rec.get("pa_datnais"),
                        "hire_date":        rec.get("pa_datent"),
                        "appointment_date": rec.get("pa_datnatu"),
                    }
                    employee_ts[mat] = rec_ts

            # ── Time dimension ────────────────────────────────────────────────
            if yr is not None and mo is not None:
                time_periods.add((int(yr), int(mo)))

            # ── Build slim fact row ───────────────────────────────────────────
            fact_row = {
                # Natural foreign keys (resolved to SKs by load_dw)
                "employee_id":  mat,
                "year_num":     int(yr) if yr is not None else None,
                "month_num":    int(mo) if mo is not None else None,
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
                "dq_has_issues":     bool(issues),
                "dq_issue_count":    len(issues),
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

            if stats["written"] % 50_000 == 0:
                log.info("  %d rows written...", stats["written"])
                _cb(min(18 + stats["written"] // 25_000, 62),
                    f"{stats['written']:,} rows processed…",
                    rows=stats["written"])

            if limit and stats["written"] >= limit:
                log.info("Row limit %d reached — stopping early (test mode)", limit)
                _cb(62, f"Test limit reached — {stats['written']:,} rows written", rows=stats["written"])
                break

    log.info("Fact file written — %d rows (%d duplicates skipped within file)",
             stats["written"], stats["duplicates_skipped"])
    _cb(63, f"Fact file complete — {stats['written']:,} rows written", rows=stats["written"])

    # ── Write dimension files ─────────────────────────────────────────────────
    _cb(65, "Writing dimension files…")
    _write_jsonl(_dim_employee,  employees.values())
    _write_jsonl(_dim_grade,     grade_lookup.values())
    _write_jsonl(_dim_nature,    nature_lookup.values())
    _write_jsonl(_dim_organisme, _org_records(org_lookup))
    _write_jsonl(_dim_region,    _region_records(region_lookup))
    _write_jsonl(_dim_time,      _time_records(time_periods))

    log.info("Dimension files written — employees=%d  time_periods=%d",
             len(employees), len(time_periods))
    _cb(68, f"Dimension files written — {len(employees):,} employees, {len(time_periods)} time periods")

    # ── Quality gate ──────────────────────────────────────────────────────────
    report = _quality_gate_and_report(
        run_id=run_id, pipeline="DW1_paie", started=started,
        source=source, stats=stats, time_periods=time_periods,
        log=log,
    )
    return report


# ── Quality gate ──────────────────────────────────────────────────────────────

def _quality_gate_and_report(run_id, pipeline, started, source, stats,
                              time_periods, log) -> dict:
    n = stats["written"] or 1
    pcts = {
        "pct_grade_matched":  round(100 * stats["grade_matched"]  / n, 2),
        "pct_nature_matched": round(100 * stats["nature_matched"] / n, 2),
        "pct_org_matched":    round(100 * stats["org_matched"]    / n, 2),
        "pct_region_matched": round(100 * stats["region_matched"] / n, 2),
        "pct_has_issues":     round(100 * stats["has_issues"]     / n, 2),
        "pct_duplicates":     round(100 * stats["duplicates_skipped"] /
                                    max(stats["total_raw"] - stats["skipped_type"], 1), 2),
    }

    qg_pass, qg_errors, qg_warns = True, [], []

    if pcts["pct_grade_matched"] / 100 < QG_GRADE_MIN_MATCH:
        qg_pass = False
        qg_errors.append(f"grade match {pcts['pct_grade_matched']}% < {QG_GRADE_MIN_MATCH*100:.0f}%")

    if pcts["pct_nature_matched"] / 100 < QG_NATURE_MIN_MATCH:
        qg_pass = False
        qg_errors.append(f"nature match {pcts['pct_nature_matched']}% < {QG_NATURE_MIN_MATCH*100:.0f}%")

    if pcts["pct_org_matched"] / 100 < QG_ORGANISME_WARN_AT:
        qg_warns.append(f"org match {pcts['pct_org_matched']}% (known limitation — partial keys)")

    if pcts["pct_region_matched"] / 100 < QG_REGION_WARN_AT:
        qg_warns.append(f"region match {pcts['pct_region_matched']}% — pa_loca has no crosswalk")

    if stats["duplicates_skipped"] > 0:
        qg_warns.append(f"{stats['duplicates_skipped']} duplicate rows removed within source file")

    status = "PASS" if qg_pass else "FAIL"
    if qg_warns:
        status += "_WITH_WARNINGS"

    if not qg_pass:
        log.error("Quality gate FAILED: %s", qg_errors)
    for w in qg_warns:
        log.warning("QG warning: %s", w)
    log.info("Quality gate: %s", status)

    report = {
        "run_id":       run_id,
        "pipeline":     pipeline,
        "started_at":   started.isoformat(),
        "finished_at":  datetime.now(timezone.utc).isoformat(),
        "source":       str(source),
        "stats":        {**stats, **pcts},
        "quality_gate": {"status": status, "errors": qg_errors, "warnings": qg_warns},
    }

    report_path = REPORTS_DIR / f"pipeline_paie_{run_id}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Report written: %s", report_path.name)
    return report


# ── Helpers ───────────────────────────────────────────────────────────────────

def _write_jsonl(path: Path, records) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(dict(rec), ensure_ascii=False, default=str) + "\n")


def _org_records(lookup: dict):
    seen = set()
    for rec in lookup["full"].values():
        key = (rec["codetab"], rec["dire"])
        if key not in seen:
            seen.add(key)
            yield rec


def _region_records(lookup: dict):
    seen = set()
    for rec in lookup["by_full"].values():
        key = (rec["coddep"], rec.get("codreg", ""))
        if key not in seen:
            seen.add(key)
            yield rec


def _time_records(periods: set[tuple[int, int]]):
    for year, month in sorted(periods):
        days_in_month = monthrange(year, month)[1]
        yield {
            "year_num":          year,
            "month_num":         month,
            "year_month":        f"{year:04d}-{month:02d}",
            "month_start_date":  f"{year:04d}-{month:02d}-01",
            "month_end_date":    f"{year:04d}-{month:02d}-{days_in_month:02d}",
            "quarter":           (month - 1) // 3 + 1,
            "semester":          1 if month <= 6 else 2,
        }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DW1 (payroll) ETL pipeline")
    parser.add_argument("--source",   type=Path, default=RAW_PAIE)
    parser.add_argument("--year-min", type=int,  default=None, help="Only process records from this year onwards")
    parser.add_argument("--year-max", type=int,  default=None, help="Only process records up to this year")
    parser.add_argument("--limit",    type=int,  default=None, help="Stop after N rows (test mode)")
    args = parser.parse_args()
    run(source=args.source, year_min=args.year_min, year_max=args.year_max, limit=args.limit)
