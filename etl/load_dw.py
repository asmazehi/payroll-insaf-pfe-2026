"""
PostgreSQL DW loader for INSAF Payroll Intelligence Platform.

Loads all clean JSONL files into the DW using psycopg2 batch inserts.
All dimension loads use ON CONFLICT UPSERT — fully idempotent, safe to
re-run any number of times without creating duplicates.

All fact loads use ON CONFLICT DO UPDATE — the last run always wins,
which means re-running after a corrected ETL pass automatically repairs data.

Run AFTER the SQL DDL scripts (01-04) have been executed.

Usage:
    python -m etl.load_dw
    python -m etl.load_dw --reset   # truncate facts before loading
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import psycopg2
import psycopg2.extras

from etl.core.config import (
    CLEAN_DIR, CLEAN_DIM_EMPLOYEE, CLEAN_DIM_GRADE, CLEAN_DIM_NATURE,
    CLEAN_DIM_ORGANISME, CLEAN_DIM_REGION, CLEAN_DIM_TIME, CLEAN_DIM_INDEMNITE,
    CLEAN_FACT_PAIE, CLEAN_FACT_INDEM, DB_CONFIG,
)
from etl.core.logger import get_logger

log = get_logger("load_dw")

# Module-level regex (not re-imported on every call)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Supplementary indem-only time periods (written by pipeline_indem)
_CLEAN_DIM_TIME_INDEM = CLEAN_DIR / "dim_time_indem.jsonl"


# ── Type coercers ─────────────────────────────────────────────────────────────

def _v(rec: dict, key: str):
    """Return None for empty/null strings, else the raw value."""
    v = rec.get(key)
    if v is None:
        return None
    if isinstance(v, str) and v.strip() in ("", "null", "NULL", "None"):
        return None
    return v


def _num(rec: dict, key: str):
    v = _v(rec, key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(rec: dict, key: str):
    v = _v(rec, key)
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _bool(rec: dict, key: str):
    v = _v(rec, key)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("true", "1", "yes")


def _date(rec: dict, key: str):
    v = _v(rec, key)
    if not v:
        return None
    s = str(v)
    return s if _DATE_RE.match(s) else None


def load_jsonl(path: Path):
    if not path.exists():
        log.warning("JSONL file not found, skipping: %s", path)
        return
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                log.warning("Bad JSON line %d in %s: %s", line_no, path.name, exc)


# ── Dimension loaders ─────────────────────────────────────────────────────────

def load_dim_employee(cur, path: Path):
    log.info("Loading dim_employee from %s", path.name)
    rows = []
    for r in load_jsonl(path):
        eid = _v(r, "employee_id")
        if not eid or eid == "UNKNOWN":
            continue
        rows.append((
            eid,
            _v(r, "last_name"),
            _v(r, "first_name"),
            _int(r, "gender"),
            _date(r, "birth_date"),
            _date(r, "hire_date"),
            _date(r, "appointment_date"),
        ))

    if not rows:
        log.warning("dim_employee: no rows to insert")
        return

    cur.executemany("""
        INSERT INTO dw.dim_employee
            (employee_id, last_name, first_name, gender, birth_date, hire_date, appointment_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (employee_id) DO UPDATE SET
            last_name        = EXCLUDED.last_name,
            first_name       = EXCLUDED.first_name,
            gender           = EXCLUDED.gender,
            birth_date       = EXCLUDED.birth_date,
            hire_date        = EXCLUDED.hire_date,
            appointment_date = EXCLUDED.appointment_date,
            dw_load_ts       = NOW()
    """, rows)
    log.info("  dim_employee: %d rows upserted", len(rows))


def load_dim_grade(cur, path: Path):
    log.info("Loading dim_grade from %s", path.name)
    rows = []
    for r in load_jsonl(path):
        code = _v(r, "grade_code")
        if not code or code == "???":
            continue
        rows.append((
            code,
            _v(r, "grade_label_fr"),
            _v(r, "grade_label_ar"),
            _v(r, "category"),
            _v(r, "class_grade"),
            _int(r, "retire_age"),
        ))

    if not rows:
        log.warning("dim_grade: no rows to insert")
        return

    cur.executemany("""
        INSERT INTO dw.dim_grade
            (grade_code, grade_label_fr, grade_label_ar, category, class_grade, retire_age)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (grade_code) DO UPDATE SET
            grade_label_fr = EXCLUDED.grade_label_fr,
            grade_label_ar = EXCLUDED.grade_label_ar,
            dw_load_ts     = NOW()
    """, rows)
    log.info("  dim_grade: %d rows upserted", len(rows))


def load_dim_nature(cur, path: Path):
    log.info("Loading dim_nature from %s", path.name)
    rows = []
    for r in load_jsonl(path):
        code = _v(r, "nature_code")
        if not code or code == "?":
            continue
        rows.append((
            code,
            _v(r, "nature_type"),
            _v(r, "nature_label_fr"),
            _v(r, "nature_label_ar"),
        ))

    if not rows:
        log.warning("dim_nature: no rows to insert")
        return

    cur.executemany("""
        INSERT INTO dw.dim_nature
            (nature_code, nature_type, nature_label_fr, nature_label_ar)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (nature_code) DO UPDATE SET
            nature_label_fr = EXCLUDED.nature_label_fr,
            nature_label_ar = EXCLUDED.nature_label_ar,
            dw_load_ts      = NOW()
    """, rows)
    log.info("  dim_nature: %d rows upserted", len(rows))


def load_dim_organisme(cur, path: Path):
    log.info("Loading dim_organisme from %s", path.name)
    rows = []
    for r in load_jsonl(path):
        codetab = _v(r, "codetab")
        dire    = _v(r, "dire")
        if not codetab or not dire or codetab == "???":
            continue
        rows.append((
            codetab, _v(r, "cab"), _v(r, "sg"), _v(r, "dg"),
            dire, _v(r, "sdir"), _v(r, "serv"), _v(r, "unite"),
            _v(r, "liborgl"), _v(r, "liborga"),
            _v(r, "codgouv"), _v(r, "deleg"), _v(r, "typstruct"),
        ))

    if not rows:
        log.warning("dim_organisme: no rows to insert")
        return

    cur.executemany("""
        INSERT INTO dw.dim_organisme
            (codetab, cab, sg, dg, dire, sdir, serv, unite,
             liborgl, liborga, codgouv, deleg, typstruct)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (codetab, dire) DO UPDATE SET
            liborgl    = EXCLUDED.liborgl,
            liborga    = EXCLUDED.liborga,
            dw_load_ts = NOW()
    """, rows)
    log.info("  dim_organisme: %d rows upserted", len(rows))


def load_dim_region(cur, path: Path):
    log.info("Loading dim_region from %s", path.name)
    rows = []
    for r in load_jsonl(path):
        coddep = _v(r, "coddep")
        codreg = _v(r, "codreg")
        if not coddep or not codreg or coddep == "???":
            continue
        rows.append((
            coddep, codreg,
            _v(r, "lib_reg"), _v(r, "lib_rega"),
            _v(r, "code_dept"), _v(r, "code_region"),
        ))

    if not rows:
        log.warning("dim_region: no rows to insert")
        return

    cur.executemany("""
        INSERT INTO dw.dim_region
            (coddep, codreg, lib_reg, lib_rega, code_dept, code_region)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (coddep, codreg) DO UPDATE SET
            lib_reg    = EXCLUDED.lib_reg,
            lib_rega   = EXCLUDED.lib_rega,
            dw_load_ts = NOW()
    """, rows)
    log.info("  dim_region: %d rows upserted", len(rows))


def _load_dim_temps_from_file(cur, path: Path) -> int:
    """Load one time-period JSONL file into dim_temps. Returns number of rows inserted."""
    rows = []
    for r in load_jsonl(path):
        yn = _int(r, "year_num")
        mn = _int(r, "month_num")
        if not yn or not mn:
            continue
        rows.append((
            yn, mn,
            _v(r, "year_month"),
            _v(r, "month_start_date"),
        ))
    if not rows:
        return 0
    cur.executemany("""
        INSERT INTO dw.dim_temps
            (year_num, month_num, year_month, month_start_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (year_num, month_num) DO NOTHING
    """, rows)
    return len(rows)


def load_dim_temps(cur, path: Path):
    log.info("Loading dim_temps from %s", path.name)
    n = _load_dim_temps_from_file(cur, path)
    log.info("  dim_temps: %d rows upserted (main)", n)

    # Also load indem-only supplementary periods
    if _CLEAN_DIM_TIME_INDEM.exists():
        n2 = _load_dim_temps_from_file(cur, _CLEAN_DIM_TIME_INDEM)
        if n2:
            log.info("  dim_temps: %d additional rows from indem-only months", n2)


def load_dim_indemnite(cur, path: Path):
    log.info("Loading dim_indemnite from %s", path.name)
    rows = []
    for r in load_jsonl(path):
        code = _v(r, "indemnite_code")
        if not code or code == "????":
            continue
        rows.append((
            code,
            _v(r, "indemnite_label_fr"),
            _v(r, "indemnite_label_fr_long"),
            _v(r, "indemnite_label_ar"),
            _v(r, "nature_flag"),
            _bool(r, "is_taxable"),
            _bool(r, "is_cnr"),
            _v(r, "zone"),
            _num(r, "arg1"),
            _num(r, "arg2"),
            _date(r, "date_entry"),
            _v(r, "insurance_code"),
        ))

    if not rows:
        log.warning("dim_indemnite: no rows to insert")
        return

    cur.executemany("""
        INSERT INTO dw.dim_indemnite
            (indemnite_code, indemnite_label_fr, indemnite_label_fr_long,
             indemnite_label_ar, nature_flag, is_taxable, is_cnr,
             zone, arg1, arg2, date_entry, insurance_code)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (indemnite_code) DO UPDATE SET
            indemnite_label_fr      = EXCLUDED.indemnite_label_fr,
            indemnite_label_fr_long = EXCLUDED.indemnite_label_fr_long,
            indemnite_label_ar      = EXCLUDED.indemnite_label_ar,
            dw_load_ts              = NOW()
    """, rows)
    log.info("  dim_indemnite: %d rows upserted", len(rows))


# ── SK map builder ────────────────────────────────────────────────────────────

def _build_maps(cur) -> tuple[dict, dict, dict, dict, dict, dict, dict]:
    """Fetch all surrogate keys from DB into in-memory dicts for fast FK resolution."""
    log.info("Building SK lookup maps from DB...")

    cur.execute("SELECT employee_id, employee_sk FROM dw.dim_employee WHERE NOT is_unknown")
    emp = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT year_num, month_num, time_sk FROM dw.dim_temps WHERE NOT is_unknown")
    time = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    cur.execute("SELECT grade_code, grade_sk FROM dw.dim_grade WHERE NOT is_unknown")
    grade = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT nature_code, nature_sk FROM dw.dim_nature WHERE NOT is_unknown")
    nature = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT codetab, dire, organisme_sk FROM dw.dim_organisme WHERE NOT is_unknown")
    org = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    # Region: map by coddep (which corresponds to pa_codmin / ministry code)
    # Take only unambiguous coddep values (unique region per ministry code).
    cur.execute("SELECT coddep, region_sk FROM dw.dim_region WHERE NOT is_unknown")
    region: dict = {}
    for coddep, sk in cur.fetchall():
        if coddep not in region:
            region[coddep] = sk

    # Indemnite: map by indemnite_code → indemnite_sk
    cur.execute("SELECT indemnite_code, indemnite_sk FROM dw.dim_indemnite WHERE NOT is_unknown")
    indem = {r[0]: r[1] for r in cur.fetchall()}

    log.info("  Maps: emp=%d  time=%d  grade=%d  nature=%d  org=%d  region=%d  indem=%d",
             len(emp), len(time), len(grade), len(nature), len(org), len(region), len(indem))
    return emp, time, grade, nature, org, region, indem


# ── Fact row builders ─────────────────────────────────────────────────────────

MEASURE_FIELDS = [
    "m_salimp", "m_salnimp", "m_salbrut", "m_brutcnr", "m_netord", "m_netpay",
    "m_cpe", "m_retrait", "m_cps", "m_capdeces", "m_avkm", "m_avlog",
    "m_rapimp", "m_rapni", "m_sub", "m_sps", "m_spl", "m_rapsalb",
]


def _build_paie_row(r: dict, emp: dict, time: dict, grade: dict,
                    nature: dict, org: dict, region: dict) -> tuple:
    emp_sk  = emp.get(_v(r, "employee_id"), 0)
    time_sk = time.get((_int(r, "year_num"), _int(r, "month_num")), 0)
    grd_sk  = grade.get(_v(r, "grade_code"), 0)
    nat_sk  = nature.get(_v(r, "nature_code"), 0)
    org_sk  = org.get((_v(r, "org_codetab"), _v(r, "org_dire")), 0)
    reg_sk  = region.get(_v(r, "org_codetab"), 0)
    measures = tuple(_num(r, f) for f in MEASURE_FIELDS)

    return (
        emp_sk, time_sk, _v(r, "pa_type") or "1",
        grd_sk, nat_sk, org_sk, reg_sk,
        _int(r, "pa_eche"), _v(r, "pa_sitfam"), _v(r, "pa_loca_raw"),
        *measures,
        _bool(r, "dq_grade_matched"),  _bool(r, "dq_nature_matched"),
        _bool(r, "dq_org_matched"),    _bool(r, "dq_region_matched"),
        _bool(r, "dq_has_issues") or False,
        _int(r, "dq_issue_count") or 0,
        _v(r, "run_id") or "unknown",
        _v(r, "source_file") or "unknown",
    )


def _build_indem_row(r: dict, emp: dict, time: dict, grade: dict,
                     nature: dict, org: dict, region: dict, indem: dict) -> tuple:
    emp_sk    = emp.get(_v(r, "employee_id"), 0)
    time_sk   = time.get((_int(r, "year_num"), _int(r, "month_num")), 0)
    grd_sk    = grade.get(_v(r, "grade_code"), 0)
    nat_sk    = nature.get(_v(r, "nature_code"), 0)
    org_sk    = org.get((_v(r, "org_codetab"), _v(r, "org_dire")), 0)
    reg_sk    = region.get(_v(r, "org_codetab"), 0)
    # Resolve actual indemnite_sk from the code stored in the fact JSONL
    indem_sk  = indem.get(_v(r, "indemnite_code"), 0)
    measures  = tuple(_num(r, f) for f in MEASURE_FIELDS)

    return (
        emp_sk, time_sk, _v(r, "pa_type") or "3",
        grd_sk, nat_sk, org_sk, reg_sk, indem_sk,
        _int(r, "pa_eche"), _v(r, "pa_sitfam"), _v(r, "pa_loca_raw"),
        *measures,
        _bool(r, "dq_grade_matched"),  _bool(r, "dq_nature_matched"),
        _bool(r, "dq_org_matched"),    _bool(r, "dq_region_matched"),
        _bool(r, "dq_has_issues") or False,
        _int(r, "dq_issue_count") or 0,
        _v(r, "run_id") or "unknown",
        _v(r, "source_file") or "unknown",
    )


# ── SQL statements ────────────────────────────────────────────────────────────

PAIE_INSERT = """
    INSERT INTO dw.fact_paie (
        employee_sk, time_sk, pa_type,
        grade_sk, nature_sk, organisme_sk, region_sk,
        pa_eche, pa_sitfam, pa_loca_raw,
        m_salimp, m_salnimp, m_salbrut, m_brutcnr, m_netord, m_netpay,
        m_cpe, m_retrait, m_cps, m_capdeces, m_avkm, m_avlog,
        m_rapimp, m_rapni, m_sub, m_sps, m_spl, m_rapsalb,
        dq_grade_matched, dq_nature_matched, dq_org_matched, dq_region_matched,
        dq_has_issues, dq_issue_count, run_id, source_file
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s
    )
    ON CONFLICT (employee_sk, time_sk, pa_type) DO UPDATE SET
        grade_sk      = EXCLUDED.grade_sk,
        nature_sk     = EXCLUDED.nature_sk,
        organisme_sk  = EXCLUDED.organisme_sk,
        region_sk     = EXCLUDED.region_sk,
        m_netpay      = EXCLUDED.m_netpay,
        m_salbrut     = EXCLUDED.m_salbrut,
        m_salimp      = EXCLUDED.m_salimp,
        m_netord      = EXCLUDED.m_netord,
        m_retrait     = EXCLUDED.m_retrait,
        dq_has_issues = EXCLUDED.dq_has_issues,
        run_id        = EXCLUDED.run_id,
        source_file   = EXCLUDED.source_file,
        load_ts       = NOW()
"""

INDEM_INSERT = """
    INSERT INTO dw.fact_indem (
        employee_sk, time_sk, pa_type,
        grade_sk, nature_sk, organisme_sk, region_sk, indemnite_sk,
        pa_eche, pa_sitfam, pa_loca_raw,
        m_salimp, m_salnimp, m_salbrut, m_brutcnr, m_netord, m_netpay,
        m_cpe, m_retrait, m_cps, m_capdeces, m_avkm, m_avlog,
        m_rapimp, m_rapni, m_sub, m_sps, m_spl, m_rapsalb,
        dq_grade_matched, dq_nature_matched, dq_org_matched, dq_region_matched,
        dq_has_issues, dq_issue_count, run_id, source_file
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s
    )
    ON CONFLICT (employee_sk, time_sk, pa_type) DO UPDATE SET
        grade_sk      = EXCLUDED.grade_sk,
        nature_sk     = EXCLUDED.nature_sk,
        organisme_sk  = EXCLUDED.organisme_sk,
        region_sk     = EXCLUDED.region_sk,
        m_netpay      = EXCLUDED.m_netpay,
        m_salbrut     = EXCLUDED.m_salbrut,
        dq_has_issues = EXCLUDED.dq_has_issues,
        run_id        = EXCLUDED.run_id,
        source_file   = EXCLUDED.source_file,
        load_ts       = NOW()
"""


# ── Fact loaders ──────────────────────────────────────────────────────────────

def load_fact_paie(cur, path: Path, maps: tuple, batch_size: int = 5000) -> int:
    emp, time, grade, nature, org, region, _ = maps
    log.info("Loading fact_paie from %s ...", path.name)
    batch, total, unresolved_emp, unresolved_time = [], 0, 0, 0

    for r in load_jsonl(path):
        row = _build_paie_row(r, emp, time, grade, nature, org, region)
        if row[0] == 0:
            unresolved_emp += 1
        if row[1] == 0:
            unresolved_time += 1
        batch.append(row)
        if len(batch) >= batch_size:
            cur.executemany(PAIE_INSERT, batch)
            total += len(batch)
            batch = []
            if total % 100_000 == 0:
                log.info("  fact_paie: %d rows loaded...", total)

    if batch:
        cur.executemany(PAIE_INSERT, batch)
        total += len(batch)

    if unresolved_emp:
        log.warning("  fact_paie: %d rows with unresolved employee_sk (-> 0/Unknown)", unresolved_emp)
    if unresolved_time:
        log.warning("  fact_paie: %d rows with unresolved time_sk (-> 0/Unknown)", unresolved_time)
    log.info("  fact_paie: %d rows loaded", total)
    return total


def load_fact_indem(cur, path: Path, maps: tuple, batch_size: int = 5000) -> int:
    emp, time, grade, nature, org, region, indem = maps
    log.info("Loading fact_indem from %s ...", path.name)
    batch, total = [], 0
    matched_emp, unmatched_emp = 0, 0
    matched_indem, unmatched_indem = 0, 0

    for r in load_jsonl(path):
        row = _build_indem_row(r, emp, time, grade, nature, org, region, indem)
        if row[0] != 0:
            matched_emp += 1
        else:
            unmatched_emp += 1
        if row[7] != 0:   # indemnite_sk at position 7
            matched_indem += 1
        else:
            unmatched_indem += 1

        batch.append(row)
        if len(batch) >= batch_size:
            cur.executemany(INDEM_INSERT, batch)
            total += len(batch)
            batch = []
            if total % 20_000 == 0:
                log.info("  fact_indem: %d rows loaded...", total)

    if batch:
        cur.executemany(INDEM_INSERT, batch)
        total += len(batch)

    pct_emp   = round(100 * matched_emp   / total, 1) if total else 0
    pct_indem = round(100 * matched_indem / total, 1) if total else 0
    log.info("  fact_indem: %d rows | emp matched: %d (%.1f%%) | indem_sk matched: %d (%.1f%%)",
             total, matched_emp, pct_emp, matched_indem, pct_indem)
    if unmatched_emp:
        log.warning("  %d indem rows have no matching employee in dim_employee (emp_sk=0)", unmatched_emp)
    if unmatched_indem:
        log.info("  %d indem rows have indemnite_sk=0 (Unknown) — source export does not carry pa_cind, expected", unmatched_indem)
    return total


# ── Main ──────────────────────────────────────────────────────────────────────

def run(reset: bool = False) -> dict:
    log.info("Connecting to PostgreSQL: %s:%s/%s",
             DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    result = {}

    try:
        with conn.cursor() as cur:
            if reset:
                log.info("Truncating fact tables (--reset flag)...")
                cur.execute("TRUNCATE dw.fact_paie, dw.fact_indem RESTART IDENTITY")

            # ── Dimensions ────────────────────────────────────────────────────
            load_dim_employee(cur,  CLEAN_DIM_EMPLOYEE);   conn.commit()
            load_dim_grade(cur,     CLEAN_DIM_GRADE);      conn.commit()
            load_dim_nature(cur,    CLEAN_DIM_NATURE);     conn.commit()
            load_dim_organisme(cur, CLEAN_DIM_ORGANISME);  conn.commit()
            load_dim_region(cur,    CLEAN_DIM_REGION);     conn.commit()
            load_dim_temps(cur,     CLEAN_DIM_TIME);       conn.commit()
            load_dim_indemnite(cur, CLEAN_DIM_INDEMNITE);  conn.commit()

            # Reset sequences after bulk dimension upsert
            for tbl, col in [
                ("dw.dim_employee",  "employee_sk"),
                ("dw.dim_grade",     "grade_sk"),
                ("dw.dim_nature",    "nature_sk"),
                ("dw.dim_organisme", "organisme_sk"),
                ("dw.dim_region",    "region_sk"),
                ("dw.dim_temps",     "time_sk"),
                ("dw.dim_indemnite", "indemnite_sk"),
            ]:
                cur.execute(f"""
                    SELECT setval(
                        pg_get_serial_sequence('{tbl}', '{col}'),
                        GREATEST((SELECT MAX({col}) FROM {tbl}), 1)
                    )
                """)
            conn.commit()
            log.info("Sequences reset")

            # ── Build SK maps ─────────────────────────────────────────────────
            maps = _build_maps(cur)

            # ── Facts ─────────────────────────────────────────────────────────
            n_paie  = load_fact_paie(cur,  CLEAN_FACT_PAIE,  maps);  conn.commit()
            n_indem = load_fact_indem(cur, CLEAN_FACT_INDEM, maps);  conn.commit()

        # ── Final row counts ──────────────────────────────────────────────────
        with conn.cursor() as cur:
            counts = {}
            for tbl in [
                "dw.dim_employee", "dw.dim_grade", "dw.dim_nature",
                "dw.dim_organisme", "dw.dim_region", "dw.dim_temps",
                "dw.dim_indemnite", "dw.fact_paie", "dw.fact_indem",
            ]:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                n = cur.fetchone()[0]
                counts[tbl] = n
                log.info("  %-30s %s rows", tbl, n)

        result = {
            "status": "success",
            "records_loaded": {"fact_paie": n_paie, "fact_indem": n_indem},
            "table_counts": counts,
        }
        log.info("DW load complete.")

    except Exception as exc:
        conn.rollback()
        log.error("Load failed — rolled back: %s", exc)
        result = {"status": "error", "error": str(exc)}
        raise
    finally:
        conn.close()

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load clean JSONL into PostgreSQL DW")
    parser.add_argument("--reset", action="store_true",
                        help="Truncate fact tables before loading (full reload)")
    args = parser.parse_args()
    run(reset=args.reset)
