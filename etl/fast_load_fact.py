"""
etl/fast_load_fact.py
=====================
Loads an existing clean fact_paie.jsonl directly into the DW — bypassing the
5-hour ETL stage when the clean file already exists from a previous (crashed) run.

Differences vs load_dw.py:
  - Uses execute_values (batch INSERT) instead of executemany → 10-50x faster
  - Extracts dim data from the JSONL itself so no dim JSONL files are needed
  - Employees inserted with DO NOTHING so existing name/date data is preserved

Usage:
    python -m etl.fast_load_fact <clean_dir>

Example:
    python -m etl.fast_load_fact C:/Users/asmaz/AppData/Local/insaf/staging/clean/7cdcc1a9
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

from etl.core.config import DB_CONFIG
from etl.core.logger import get_logger

log = get_logger("fast_load_fact")

BATCH = 50_000   # rows per execute_values call


# ── Helpers ───────────────────────────────────────────────────────────────────

def _v(r, k):
    v = r.get(k)
    if v is None: return None
    if isinstance(v, str) and v.strip() in ("", "null", "NULL", "None"): return None
    return v

def _num(r, k):
    v = _v(r, k)
    try: return float(v) if v is not None else None
    except (TypeError, ValueError): return None

def _int(r, k):
    v = _v(r, k)
    try: return int(float(v)) if v is not None else None
    except (TypeError, ValueError): return None

def _bool(r, k):
    v = _v(r, k)
    if v is None: return None
    if isinstance(v, bool): return v
    return str(v).lower() in ("true", "1", "yes")

MEASURES = [
    "m_salimp","m_salnimp","m_salbrut","m_brutcnr","m_netord","m_netpay",
    "m_cpe","m_retrait","m_cps","m_capdeces","m_avkm","m_avlog",
    "m_rapimp","m_rapni","m_sub","m_sps","m_spl","m_rapsalb",
]


# ── Step 1: scan JSONL and upsert new dims ────────────────────────────────────

def scan_and_upsert_dims(conn, path: Path):
    log.info("Scanning %s for unique dimension values…", path.name)
    employees, time_periods = set(), set()

    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if eid := _v(r, "employee_id"):
                employees.add(eid)
            y, m = _int(r, "year_num"), _int(r, "month_num")
            if y and m:
                time_periods.add((y, m))

    log.info("  Found %d unique employees, %d time periods", len(employees), len(time_periods))

    import datetime
    with conn.cursor() as cur:
        # Employees — DO NOTHING keeps existing names/dates intact
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dw.dim_employee (employee_id, is_unknown)
            VALUES %s
            ON CONFLICT (employee_id) DO NOTHING
        """, [(eid, False) for eid in employees], page_size=10_000)
        log.info("  dim_employee: upserted (DO NOTHING) %d ids", len(employees))
    conn.commit()

    with conn.cursor() as cur:
        # Time periods — year_month is 'YYYY-MM', month_start_date is first of month
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dw.dim_temps (year_num, month_num, year_month, month_start_date)
            VALUES %s
            ON CONFLICT (year_num, month_num) DO NOTHING
        """, [
            (y, m, f"{y}-{m:02d}", datetime.date(y, m, 1))
            for y, m in time_periods
        ], page_size=1_000)
        log.info("  dim_temps: upserted %d periods", len(time_periods))
    conn.commit()


# ── Step 2: build SK maps from DB ─────────────────────────────────────────────

def build_maps(conn):
    log.info("Building SK maps from DB…")
    with conn.cursor() as cur:
        cur.execute("SELECT employee_id, employee_sk FROM dw.dim_employee WHERE NOT is_unknown")
        emp = dict(cur.fetchall())
        cur.execute("SELECT year_num, month_num, time_sk FROM dw.dim_temps WHERE NOT is_unknown")
        time = {(y, m): sk for y, m, sk in cur.fetchall()}
        cur.execute("SELECT grade_code, grade_sk FROM dw.dim_grade WHERE NOT is_unknown")
        grade = dict(cur.fetchall())
        cur.execute("SELECT nature_code, nature_sk FROM dw.dim_nature WHERE NOT is_unknown")
        nature = dict(cur.fetchall())
        cur.execute("SELECT codetab, dire, organisme_sk FROM dw.dim_organisme WHERE NOT is_unknown")
        org = {(c, d): sk for c, d, sk in cur.fetchall()}
        cur.execute("SELECT coddep, region_sk FROM dw.dim_region WHERE NOT is_unknown")
        region = dict(cur.fetchall())
    log.info("  emp=%d  time=%d  grade=%d  nature=%d  org=%d  region=%d",
             len(emp), len(time), len(grade), len(nature), len(org), len(region))
    return emp, time, grade, nature, org, region


# ── Step 3: fast bulk load ────────────────────────────────────────────────────

FACT_SQL = """
    INSERT INTO dw.fact_paie (
        employee_sk, time_sk, pa_type,
        grade_sk, nature_sk, organisme_sk, region_sk,
        pa_eche, pa_sitfam, pa_loca_raw,
        m_salimp, m_salnimp, m_salbrut, m_brutcnr, m_netord, m_netpay,
        m_cpe, m_retrait, m_cps, m_capdeces, m_avkm, m_avlog,
        m_rapimp, m_rapni, m_sub, m_sps, m_spl, m_rapsalb,
        dq_grade_matched, dq_nature_matched, dq_org_matched, dq_region_matched,
        dq_has_issues, dq_issue_count, run_id, source_file
    ) VALUES %s
    ON CONFLICT (employee_sk, time_sk, pa_type) DO UPDATE SET
        grade_sk=EXCLUDED.grade_sk, nature_sk=EXCLUDED.nature_sk,
        organisme_sk=EXCLUDED.organisme_sk, region_sk=EXCLUDED.region_sk,
        m_netpay=EXCLUDED.m_netpay, m_salbrut=EXCLUDED.m_salbrut,
        m_salimp=EXCLUDED.m_salimp, m_netord=EXCLUDED.m_netord,
        m_retrait=EXCLUDED.m_retrait, dq_has_issues=EXCLUDED.dq_has_issues,
        run_id=EXCLUDED.run_id, source_file=EXCLUDED.source_file,
        load_ts=NOW()
"""


def fast_load(conn, path: Path, maps):
    emp, time, grade, nature, org, region = maps
    log.info("Loading fact_paie from %s  (%s)…", path.name,
             f"{path.stat().st_size/1e9:.1f} GB")

    batch, total, t0 = [], 0, time_module.time()

    def flush(cur, rows):
        psycopg2.extras.execute_values(cur, FACT_SQL, rows, page_size=BATCH)
        conn.commit()

    with open(path, encoding="utf-8") as f, conn.cursor() as cur:
        for line in f:
            r = json.loads(line)
            row = (
                emp.get(_v(r,"employee_id"), 0),
                time.get((_int(r,"year_num"), _int(r,"month_num")), 0),
                _v(r,"pa_type") or "1",
                grade.get(_v(r,"grade_code"), 0),
                nature.get(_v(r,"nature_code"), 0),
                org.get((_v(r,"org_codetab"), _v(r,"org_dire")), 0),
                region.get(_v(r,"org_codetab"), 0),
                _int(r,"pa_eche"), _v(r,"pa_sitfam"), _v(r,"pa_loca_raw"),
                *[_num(r,m) for m in MEASURES],
                _bool(r,"dq_grade_matched"), _bool(r,"dq_nature_matched"),
                _bool(r,"dq_org_matched"),   _bool(r,"dq_region_matched"),
                _bool(r,"dq_has_issues") or False,
                _int(r,"dq_issue_count") or 0,
                _v(r,"run_id") or "unknown",
                _v(r,"source_file") or "unknown",
            )
            batch.append(row)
            if len(batch) >= BATCH:
                flush(cur, batch)
                total += len(batch)
                batch.clear()
                elapsed = time_module.time() - t0
                rate = total / elapsed if elapsed else 0
                remaining = (40_265_317 - total) / rate if rate else 0
                log.info("  %10d rows loaded  (%.0f rows/s  ~%.0f min remaining)",
                         total, rate, remaining / 60)

        if batch:
            flush(cur, batch)
            total += len(batch)

    elapsed = time_module.time() - t0
    log.info("fact_paie: %d rows loaded in %.1f min", total, elapsed / 60)
    return total


# ── Entry point ───────────────────────────────────────────────────────────────

import time as time_module   # avoid shadowing the 'time' dict variable

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m etl.fast_load_fact <clean_dir>")
        print("Example: python -m etl.fast_load_fact "
              "C:/Users/asmaz/AppData/Local/insaf/staging/clean/7cdcc1a9")
        sys.exit(1)

    clean_dir = Path(sys.argv[1])
    fact_file = clean_dir / "fact_paie.jsonl"
    if not fact_file.exists():
        print(f"ERROR: not found: {fact_file}")
        sys.exit(1)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        scan_and_upsert_dims(conn, fact_file)
        maps = build_maps(conn)
        total = fast_load(conn, fact_file, maps)
        log.info("Done. %d rows in fact_paie.", total)

        # Final counts
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_paie")
            log.info("dw.fact_paie total: %d rows", cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM dw.dim_employee WHERE NOT is_unknown")
            log.info("dw.dim_employee total: %d employees", cur.fetchone()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
