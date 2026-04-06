"""
Central configuration for the INSAF Payroll Intelligence Platform.
All paths, constants, and thresholds live here — never hardcoded elsewhere.
"""
from pathlib import Path

# ── Project root ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# ── Raw source files ─────────────────────────────────────────────────────────
RAW_DIR       = BASE_DIR / "data" / "raw"
RAW_PAIE      = RAW_DIR / "paie2015.json"       # payroll type 1  (~1 GB)
RAW_INDEM     = RAW_DIR / "ind2015.json"         # indemnities type 3 (~75 MB)
RAW_INDEM_DEF = RAW_DIR / "indem_def.json"       # indemnity code reference
RAW_GRADE     = RAW_DIR / "grade.json"
RAW_ORGANISME = RAW_DIR / "organisme.json"
RAW_REGION    = RAW_DIR / "region.json"
RAW_NATURE    = RAW_DIR / "nature.json"

# ── Clean output files ────────────────────────────────────────────────────────
CLEAN_DIR         = BASE_DIR / "data" / "clean"
CLEAN_FACT_PAIE   = CLEAN_DIR / "fact_paie.jsonl"
CLEAN_FACT_INDEM  = CLEAN_DIR / "fact_indem.jsonl"
CLEAN_DIM_EMPLOYEE   = CLEAN_DIR / "dim_employee.jsonl"
CLEAN_DIM_GRADE      = CLEAN_DIR / "dim_grade.jsonl"
CLEAN_DIM_NATURE     = CLEAN_DIR / "dim_nature.jsonl"
CLEAN_DIM_ORGANISME  = CLEAN_DIR / "dim_organisme.jsonl"
CLEAN_DIM_REGION     = CLEAN_DIR / "dim_region.jsonl"
CLEAN_DIM_TIME       = CLEAN_DIR / "dim_time.jsonl"
CLEAN_DIM_INDEMNITE  = CLEAN_DIR / "dim_indemnite.jsonl"

# ── Reports ───────────────────────────────────────────────────────────────────
REPORTS_DIR = BASE_DIR / "reports"

# ── ETL behaviour ─────────────────────────────────────────────────────────────
PAIE_TYPE_FILTER  = "1"   # pa_type value for DW1
INDEM_TYPE_FILTER = "3"   # pa_type value for DW2

# Date century pivot: YY <= PIVOT → 20YY, YY > PIVOT → 19YY
DATE_CENTURY_PIVOT = 30

# ── Organisme matching ────────────────────────────────────────────────────────
# Minimum fields needed to attempt an organisme join.
# We do NOT fill blanks with "000" — we match on what we have.
ORGANISME_MIN_JOIN_FIELDS = ["codetab", "dire"]

# ── Quality gate thresholds (fail pipeline if below) ─────────────────────────
QG_GRADE_MIN_MATCH    = 0.95
QG_NATURE_MIN_MATCH   = 0.99
QG_ORGANISME_WARN_AT  = 0.05   # warn only — known-low due to partial keys
QG_REGION_WARN_AT     = 0.01   # warn only — pa_loca has no crosswalk

# ── PostgreSQL connection (overridden by env vars in production) ──────────────
import os
DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "payroll_dw"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}
