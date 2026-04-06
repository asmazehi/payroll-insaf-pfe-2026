"""
Field normalization for INSAF payroll and indemnity data.

Rules:
  - Decimal separator comma → dot   ("1923,452" → 1923.452)
  - Dates DD/MM/YY → ISO YYYY-MM-DD  (with configurable century pivot)
  - Codes: strip + uppercase
  - Names: collapse internal whitespace, preserve original case
  - Empty strings → None  (never fabricate a value)
  - Zeros in source stay zero  (never confuse "absent" with zero)

Returns None for any field that cannot be parsed —
the caller decides whether to flag or skip, NOT this module.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Optional

from etl.core.config import DATE_CENTURY_PIVOT

# ── Salary/monetary fields ────────────────────────────────────────────────────
SALARY_FIELDS: frozenset[str] = frozenset({
    "pa_salimp", "pa_salnimp", "pa_salbrut", "pa_brutcnr",
    "pa_netord",  "pa_netpay",
    "pa_cpe",     "pa_retrait", "pa_cps",    "pa_capdeces",
    "pa_avkm",    "pa_avlog",
    "pa_rapimp",  "pa_rapni",   "pa_sub",    "pa_sps",
    "pa_spl",     "pa_rapsalb",
})

# ── Date fields ───────────────────────────────────────────────────────────────
DATE_FIELDS: frozenset[str] = frozenset({
    "pa_datnais", "pa_datent", "pa_datnatu", "pa_date_ech",
})

# ── Org hierarchy fields (3-char codes) ───────────────────────────────────────
ORG_FIELDS: frozenset[str] = frozenset({
    "pa_cab", "pa_sg", "pa_dg", "pa_dire",
    "pa_sdir", "pa_serv", "pa_unite",
})

_DATE_RE = re.compile(r"^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2,4})$")


# ── Primitive parsers ─────────────────────────────────────────────────────────

def parse_decimal(value: object) -> Optional[float]:
    """
    Parse a French-format decimal string to float.
    "1923,452" → 1923.452
    "0"        → 0.0
    ""  / None → None   (absent, not zero)
    """
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "null", "NULL"):
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None   # caller will flag this


def parse_date(value: object) -> Optional[str]:
    """
    Parse DD/MM/YY or DD/MM/YYYY (and - or . separators) to ISO YYYY-MM-DD.
    Returns None if value is absent or unparseable — never fabricates a date.

    Century pivot (configurable in config.py):
      YY ≤ DATE_CENTURY_PIVOT  →  20YY
      YY >  DATE_CENTURY_PIVOT →  19YY
    """
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = _DATE_RE.match(s)
    if not m:
        return None

    day, month, year_s = int(m.group(1)), int(m.group(2)), m.group(3)

    if len(year_s) == 2:
        yy = int(year_s)
        year = (2000 + yy) if yy <= DATE_CENTURY_PIVOT else (1900 + yy)
    else:
        year = int(year_s)

    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    if not (1900 <= year <= 2099):
        return None

    try:
        date(year, month, day)          # validates day-in-month
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return None


def normalize_code(value: object, pad_to: int = None) -> Optional[str]:
    """
    Normalize a reference code: strip whitespace, uppercase.
    Optionally zero-pad to *pad_to* length.
    Returns None for empty / None input.
    """
    if value is None:
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    if pad_to and len(s) < pad_to:
        s = s.zfill(pad_to)
    return s


def normalize_name(value: object) -> Optional[str]:
    """
    Collapse internal whitespace in a person name.
    Preserves original case — UPPER/lower choice is for the display layer.
    """
    if not value:
        return None
    s = " ".join(str(value).split())
    return s if s else None


def to_int(value: object) -> Optional[int]:
    """Cast to int or return None."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# ── Full payroll record normalizer ────────────────────────────────────────────

def normalize_payroll_record(raw: dict) -> tuple[dict, list[str]]:
    """
    Normalize one raw payroll record (from paie2015.json or ind2015.json).

    Returns
    -------
    (normalized_dict, issues_list)
      issues_list: list of strings describing fields that could not be parsed.
                   Caller stores these in dq_issues — nothing is silently dropped.
    """
    issues: list[str] = []

    # ── Key normalization ─────────────────────────────────────────────────────
    # Raw keys may be uppercase (Oracle export) or have typos
    rec: dict = {}
    for k, v in raw.items():
        clean_key = k.lower().strip().replace(" ", "_")   # fixes "pa_anne e"
        rec[clean_key] = v

    out: dict = {}

    # ── Identity ──────────────────────────────────────────────────────────────
    out["pa_mat"]    = normalize_code(rec.get("pa_mat"))
    out["pa_codmin"] = normalize_code(rec.get("pa_codmin"), pad_to=3)
    out["pa_type"]   = str(rec.get("pa_type", "")).strip()
    out["pa_mois"]   = to_int(rec.get("pa_mois"))
    out["pa_annee"]  = to_int(rec.get("pa_annee"))
    out["pa_sec"]    = to_int(rec.get("pa_sec"))

    # ── Person ────────────────────────────────────────────────────────────────
    out["pa_noml"]  = normalize_name(rec.get("pa_noml"))
    out["pa_prenl"] = normalize_name(rec.get("pa_prenl"))

    raw_sexe = str(rec.get("pa_sexe", "")).strip()
    if raw_sexe in ("1", "2"):
        out["pa_sexe"] = int(raw_sexe)
    else:
        out["pa_sexe"] = None
        if raw_sexe:
            issues.append(f"pa_sexe:invalid:{raw_sexe!r}")

    # ── Dates ─────────────────────────────────────────────────────────────────
    for field in DATE_FIELDS:
        raw_val = rec.get(field)
        parsed  = parse_date(raw_val)
        out[field] = parsed
        if raw_val and not parsed:
            issues.append(f"{field}:unparseable:{raw_val!r}")

    # ── Salary / monetary ─────────────────────────────────────────────────────
    for field in SALARY_FIELDS:
        raw_val = rec.get(field)
        parsed  = parse_decimal(raw_val)
        out[field] = parsed
        if raw_val is not None and parsed is None:
            # raw_val exists but could not be parsed — flag it
            s = str(raw_val).strip()
            if s not in ("", "null", "NULL"):
                issues.append(f"{field}:unparseable:{raw_val!r}")

    # ── Grade / nature ────────────────────────────────────────────────────────
    out["pa_grd"]  = normalize_code(rec.get("pa_grd"),  pad_to=3)
    out["pa_natu"] = normalize_code(rec.get("pa_natu"))
    out["pa_eche"] = to_int(rec.get("pa_eche"))

    # ── Org hierarchy ─────────────────────────────────────────────────────────
    for field in ORG_FIELDS:
        out[field] = normalize_code(rec.get(field), pad_to=3)

    out["pa_loca"]    = normalize_code(rec.get("pa_loca"))
    out["pa_indice"]  = rec.get("pa_indice")        # keep as-is
    out["pa_sitfam"]  = str(rec.get("pa_sitfam", "")).strip() or None
    out["pa_nbrfam"]  = str(rec.get("pa_nbrfam", "")).strip() or None
    out["pa_enfits"]  = to_int(rec.get("pa_enfits"))
    out["pa_totinf"]  = to_int(rec.get("pa_totinf"))

    # ── Pass-through fields ───────────────────────────────────────────────────
    for field in (
        "pa_adrl", "pa_regcnr", "pa_capd", "pa_article", "pa_parag",
        "pa_mp", "pa_idbank", "pa_codconj", "pa_efonc", "pa_fonc",
        "pa_mutuel", "pa_typarmee",
    ):
        val = rec.get(field)
        out[field] = str(val).strip() if val is not None else None

    # ── Data quality metadata ─────────────────────────────────────────────────
    out["dq_issues"]      = issues
    out["dq_has_issues"]  = bool(issues)
    out["dq_issue_count"] = len(issues)

    return out, issues
