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
    RAW_ETABLISSEMENT,
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


def load_dim_etablissement(cur, path: Path):
    """
    Load dim_etablissement from the Oracle-export JSON (data/newRawData/etablissement.json).
    Format: { "results": [{ "columns": [...], "items": [...] }] }
    Populates natorg, codtutel, codchap, codsec — required for the v_ministry_codetabs view.
    """
    if not path.exists():
        log.warning("etablissement.json not found, skipping: %s", path)
        return

    log.info("Loading dim_etablissement from %s", path.name)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        log.warning("Could not parse etablissement.json: %s", exc)
        return

    # Handle Oracle-export format: { "results": [{ "items": [...] }] }
    if isinstance(data, dict) and "results" in data:
        items = data["results"][0].get("items", []) if data["results"] else []
    elif isinstance(data, list):
        items = data
    else:
        log.warning("Unexpected etablissement.json format")
        return

    rows = []
    for r in items:
        codetab = _v(r, "codetab") or _v(r, "CODETAB")
        if not codetab or codetab == "???":
            continue
        codetab = str(codetab).strip().upper()[:3]

        def _f(key):
            v = r.get(key) or r.get(key.upper())
            return str(v).strip() if v is not None and str(v).strip() else None

        rows.append((
            codetab,
            _f("natorg"),
            _f("libcetabl"),
            _f("libcetaba"),
            _f("libletabl"),
            _f("libletaba"),
            _f("sigle_etab"),
            _f("typgest"),
            _f("codgest"),
            _f("adretabl"),
            _f("adretaba"),
            _f("teletab"),
            _f("resp_etabl"),
            _f("resp_etaba"),
            _f("etat_etab"),
            _f("code_resp"),
            _f("stutel"),
            _f("codtutel"),
            _f("codchap"),
            _f("codsec"),
            _f("subv"),
        ))

    if not rows:
        log.warning("dim_etablissement: no rows to insert")
        return

    cur.executemany("""
        INSERT INTO dw.dim_etablissement
            (codetab, natorg, libcetabl, libcetaba, libletabl, libletaba,
             sigle_etab, typgest, codgest, adretabl, adretaba, teletab,
             resp_etabl, resp_etaba, etat_etab, code_resp, stutel,
             codtutel, codchap, codsec, subv)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (codetab) DO UPDATE SET
            natorg     = EXCLUDED.natorg,
            libletabl  = COALESCE(EXCLUDED.libletabl, dw.dim_etablissement.libletabl),
            libletaba  = COALESCE(EXCLUDED.libletaba, dw.dim_etablissement.libletaba),
            libcetabl  = COALESCE(EXCLUDED.libcetabl, dw.dim_etablissement.libcetabl),
            sigle_etab = COALESCE(EXCLUDED.sigle_etab, dw.dim_etablissement.sigle_etab),
            codtutel   = COALESCE(EXCLUDED.codtutel,   dw.dim_etablissement.codtutel),
            codchap    = COALESCE(EXCLUDED.codchap,    dw.dim_etablissement.codchap),
            codsec     = COALESCE(EXCLUDED.codsec,     dw.dim_etablissement.codsec),
            dw_load_ts = NOW()
    """, rows)
    log.info("  dim_etablissement: %d rows upserted", len(rows))

    # ── Keyword-based ministry assignment ─────────────────────────────────────
    _assign_ministry_by_keywords(cur)

    # ── Deduplicate sub-entities that were incorrectly marked natorg='1' ──────
    # B10, B30 = sub-entities of B00 (Présidence de la République)
    cur.execute("""
        UPDATE dw.dim_etablissement
        SET codtutel = 'B00'
        WHERE codetab IN ('B10', 'B30')
          AND (codtutel IS NULL OR codtutel = '')
    """)


def _assign_ministry_by_keywords(cur) -> None:
    """
    Assign codtutel (parent ministry) to establishments using keyword matching.
    Only touches rows where codtutel is still NULL — preserves explicit assignments.
    """
    RULES = [
        # (ministry_codetab, natorg_filter_or_None, [keyword_conditions], [exclusion_conditions])
        # Municipalities → Interior
        ("F00",  "09", [], []),
        # Health
        ("H00",  None,
         ["libletabl ILIKE '%hopital%'", "libletabl ILIKE '%clinique%'",
          "libletabl ILIKE '%infirmeri%'", "libletabl ILIKE '%dispensaire%'",
          "libletabl ILIKE '%maternite%'", "libletabl ILIKE '%pharmacie%'",
          "libletabl ILIKE '%sanitaire%'"], []),
        # Schools (not higher ed)
        ("R00",  None,
         ["libletabl ILIKE '%ecole%'", "libletabl ILIKE '%lycee%'",
          "libletabl ILIKE '%college%'", "libletabl ILIKE '%primaire%'",
          "libletabl ILIKE '%preparatoire%'"],
         ["libletabl ILIKE '%superieur%'", "libletabl ILIKE '%universite%'",
          "libletabl ILIKE '%iset%'"]),
        # Higher education / research
        ("S00",  None,
         ["libletabl ILIKE '%universite%'", "libletabl ILIKE '%faculte%'",
          "libletabl ILIKE '%ecole superieure%'", "libletabl ILIKE '%ecole nationale%'",
          "libletabl ILIKE '%iset%'", "libletabl ILIKE '%inat%'",
          "libletabl ILIKE '%institut superieur%'",
          "libletabl ILIKE '%centre de recherche%'"], []),
        # Agriculture
        ("M00",  None,
         ["libletabl ILIKE '%agriculture%'", "libletabl ILIKE '%agricole%'",
          "libletabl ILIKE '%peche%'", "libletabl ILIKE '%foret%'",
          "libletabl ILIKE '%elevage%'", "libletabl ILIKE '%hydraulique%'",
          "libletabl ILIKE '%crda%'", "libletabl ILIKE '%barrage%'",
          "libletabl ILIKE '%veterinaire%'"], []),
        # Transport companies
        ("X00",  None,
         ["libletabl ILIKE '%societe%transport%'", "libletabl ILIKE '%transports%'"],
         ["libletabl ILIKE '%hydrocarbure%'", "libletabl ILIKE '%petrolier%'"]),
        # Finance / tax / customs
        ("J00",  None,
         ["libletabl ILIKE '%tresor%'", "libletabl ILIKE '%recette%'",
          "libletabl ILIKE '%impots%'", "libletabl ILIKE '%douane%'",
          "libletabl ILIKE '%tresorerie%'", "libletabl ILIKE '%fisc%'"], []),
        # Justice
        ("D00",  None,
         ["libletabl ILIKE '%tribunal%'", "libletabl ILIKE '%prison%'",
          "libletabl ILIKE '%penitentiaire%'", "libletabl ILIKE '%judiciaire%'"], []),
        # Defense
        ("G00",  None,
         ["libletabl ILIKE '%militaire%'", "libletabl ILIKE '%armee%'",
          "libletabl ILIKE '%gendarmerie%'"], []),
        # Cultural affairs
        ("L00",  None,
         ["libletabl ILIKE '%musee%'", "libletabl ILIKE '%bibliotheque%'",
          "libletabl ILIKE '%theatre%'", "libletabl ILIKE '%patrimoine%'",
          "libletabl ILIKE '%culturel%'", "libletabl ILIKE '%cinema%'",
          "libletabl ILIKE '%radio%'", "libletabl ILIKE '%television%'"], []),
        # Social affairs
        ("V00",  None,
         ["libletabl ILIKE '%securite sociale%'", "libletabl ILIKE '%cnss%'",
          "libletabl ILIKE '%cnrps%'", "libletabl ILIKE '%assistance sociale%'",
          "libletabl ILIKE '%orphelinat%'", "libletabl ILIKE '%maison de retraite%'",
          "libletabl ILIKE '%handicap%'"], []),
        # Tourism
        ("500",  None,
         ["libletabl ILIKE '%tourisme%'", "libletabl ILIKE '%touristique%'"], []),
        # ICT
        ("P00",  None,
         ["libletabl ILIKE '%informatique%'", "libletabl ILIKE '%numerique%'",
          "libletabl ILIKE '%telecommunication%'"], []),
        # Religious affairs
        ("300",  None,
         ["libletabl ILIKE '%mosquee%'", "libletabl ILIKE '%habous%'",
          "libletabl ILIKE '%culte%'", "libletabl ILIKE '%religieux%'"], []),
        # Equipment / housing
        ("N00",  None,
         ["libletabl ILIKE '%equipement%'", "libletabl ILIKE '%habitat%'",
          "libletabl ILIKE '%logement%'", "libletabl ILIKE '%aeroport%'"], []),
        # Industry / energy / mines
        ("Y10",  None,
         ["libletabl ILIKE '%industriel%'", "libletabl ILIKE '%energie%'",
          "libletabl ILIKE '%electricite%'", "libletabl ILIKE '%hydrocarbure%'",
          "libletabl ILIKE '%petrole%'", "libletabl ILIKE '%phosphate%'",
          "libletabl ILIKE '%mines%'", "libletabl ILIKE '%chimique%'"],
         ["libletabl ILIKE '%transport%'"]),
        # Employment / vocational training
        ("600",  None,
         ["libletabl ILIKE '%formation professionnelle%'", "libletabl ILIKE '%atfp%'",
          "libletabl ILIKE '%aneti%'", "libletabl ILIKE '%centre de formation%'"], []),
        # Environment
        ("400",  None,
         ["libletabl ILIKE '%environnement%'", "libletabl ILIKE '%onas%'",
          "libletabl ILIKE '%anpe%'", "libletabl ILIKE '%pollution%'",
          "libletabl ILIKE '%dechets%'"], []),
        # Family / women / children
        ("YJ0",  None,
         ["libletabl ILIKE '%famille%'", "libletabl ILIKE '%enfance%'",
          "libletabl ILIKE '%creche%'", "libletabl ILIKE '%garderie%'",
          "libletabl ILIKE '%maison d%enfant%'"], []),
        # Sport federations (catches names not covered by natorg='8')
        ("W00",  None,
         ["libletabl ILIKE '%federation tunisienne%sport%'",
          "libletabl ILIKE '%federation tunisienne de%ball%'",
          "libletabl ILIKE '%federation tunisienne de football%'",
          "libletabl ILIKE '%federation tunisienne de basket%'",
          "libletabl ILIKE '%federation tunisienne de volley%'",
          "libletabl ILIKE '%federation tunisienne de natation%'",
          "libletabl ILIKE '%federation tunisienne de tennis%'",
          "libletabl ILIKE '%federation tunisienne d%athlet%'",
          "libletabl ILIKE '%federation tunisienne des sports%'",
          "libletabl ILIKE '%comite national olympique%'",
          "libletabl ILIKE '%commissariat general au sport%'"], []),
    ]

    total = 0
    for ministry, natorg_filter, keywords, exclusions in RULES:
        cur.execute("SELECT 1 FROM dw.dim_etablissement WHERE codetab = %s", (ministry,))
        if not cur.fetchone():
            continue

        if natorg_filter:
            where = f"natorg = '{natorg_filter}' AND (codtutel IS NULL OR codtutel = '')"
        else:
            if not keywords:
                continue
            kw_or = " OR ".join(keywords)
            where = f"natorg != '1' AND (codtutel IS NULL OR codtutel = '') AND ({kw_or})"

        if exclusions:
            ex_and = " AND ".join(f"NOT ({e})" for e in exclusions)
            where += f" AND {ex_and}"

        where_escaped = where.replace("%", "%%")
        cur.execute(f"UPDATE dw.dim_etablissement SET codtutel = %s WHERE {where_escaped}", (ministry,))
        if cur.rowcount:
            total += cur.rowcount

    if total:
        log.info("  dim_etablissement: %d establishments linked to parent ministries via keywords", total)


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


def load_dim_temps(cur, path: Path, time_indem_path: Path | None = None):
    log.info("Loading dim_temps from %s", path.name)
    n = _load_dim_temps_from_file(cur, path)
    log.info("  dim_temps: %d rows upserted (main)", n)

    # Also load indem-only supplementary periods
    _ti = time_indem_path or _CLEAN_DIM_TIME_INDEM
    if _ti.exists():
        n2 = _load_dim_temps_from_file(cur, _ti)
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


def load_fact_indem(cur, path: Path, maps: tuple, batch_size: int = 5000,
                    progress_cb=None) -> int:
    emp, time, grade, nature, org, region, indem = maps
    log.info("Loading fact_indem from %s ...", path.name)
    batch, total = [], 0
    matched_emp, unmatched_emp = 0, 0
    matched_indem, unmatched_indem = 0, 0
    _cb = progress_cb or (lambda pct, msg, **kw: None)

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
                _cb(93, f"Loading fact_indem… {total:,} rows written", rows=total)

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

def run(reset: bool = False, clean_dir: Path | None = None, progress_cb=None) -> dict:
    _d = clean_dir or CLEAN_DIR
    _dim_employee  = _d / "dim_employee.jsonl"
    _dim_grade     = _d / "dim_grade.jsonl"
    _dim_nature    = _d / "dim_nature.jsonl"
    _dim_organisme = _d / "dim_organisme.jsonl"
    _dim_region    = _d / "dim_region.jsonl"
    _dim_time      = _d / "dim_time.jsonl"
    _dim_indemnite = _d / "dim_indemnite.jsonl"
    _fact_paie     = _d / "fact_paie.jsonl"
    _fact_indem    = _d / "fact_indem.jsonl"
    _dim_time_indem = _d / "dim_time_indem.jsonl"

    log.info("Connecting to PostgreSQL: %s:%s/%s  clean_dir=%s",
             DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"], _d)

    _cb = progress_cb or (lambda pct, msg, **kw: None)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    result = {}

    try:
        with conn.cursor() as cur:
            if reset:
                log.info("Truncating fact tables (--reset flag)...")
                cur.execute("TRUNCATE dw.fact_paie, dw.fact_indem RESTART IDENTITY")

            # ── Dimensions ────────────────────────────────────────────────────
            _cb(80, "Loading employee dimension…")
            load_dim_employee(cur,  _dim_employee);   conn.commit()
            _cb(81, "Loading grade and nature dimensions…")
            load_dim_grade(cur,     _dim_grade);      conn.commit()
            load_dim_nature(cur,    _dim_nature);     conn.commit()
            _cb(82, "Loading organisme, region, and etablissement dimensions…")
            load_dim_organisme(cur,    _dim_organisme);         conn.commit()
            load_dim_region(cur,       _dim_region);            conn.commit()
            load_dim_etablissement(cur, RAW_ETABLISSEMENT);     conn.commit()
            _cb(83, "Loading time and indemnite dimensions…")
            load_dim_temps(cur,     _dim_time, _dim_time_indem);  conn.commit()
            load_dim_indemnite(cur, _dim_indemnite);  conn.commit()

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
            _cb(86, "Building surrogate key maps…")
            maps = _build_maps(cur)

            # ── Facts ─────────────────────────────────────────────────────────
            _cb(88, "Loading fact_paie into DW (large table — may take a few minutes)…")
            n_paie  = load_fact_paie(cur,  _fact_paie,  maps);  conn.commit()
            _cb(93, f"fact_paie loaded — {n_paie:,} rows. Loading fact_indem…")
            n_indem = load_fact_indem(cur, _fact_indem, maps, progress_cb=_cb);  conn.commit()

            # Refresh materialized views now that fact tables are up to date.
            # CONCURRENTLY allows reads during refresh (no exclusive lock).
            _cb(96, "Refreshing materialized views…")
            for mv in ["dw.mv_ministry_details", "dw.mv_payroll_by_month",
                       "dw.mv_grade_distribution", "dw.mv_grade_by_ministry"]:
                try:
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                    conn.commit()
                    log.info("  Refreshed %s", mv)
                except Exception as mv_err:
                    conn.rollback()
                    log.warning("  Could not refresh %s: %s", mv, mv_err)
            _cb(97, f"DW load complete — {n_paie + n_indem:,} total rows written")

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
