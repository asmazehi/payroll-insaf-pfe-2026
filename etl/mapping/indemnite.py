"""
Indemnity code reference mapper — links indem_def.json to DW2 fact_indem.

indem_def fields
────────────────
  tmi_cind  — indemnity code (4-digit string, e.g. "0425")
  tmi_libc  — short French label  ("RAP. FONC.")
  tmi_libl  — long French label   ("RAP. IND. LIEE FONCTION")
  tmi_liba  — Arabic label        (mojibake — will be fixed)
  tmi_nat   — nature flag ("F" = financial, etc.)
  tmi_nai   — 0/1 flag
  tmi_pflag — priority/classification flag
  tmi_dpc   — date of entry (DD/MM/YY)
  tmi_zon   — zone code
  tmi_arg1  — argument 1 (multiplier / coefficient)
  tmi_arg2  — argument 2
  tmi_cins  — insurance code
  tmi_cnr   — CNR flag ("1" = yes)
  tmi_imp   — taxable flag ("1" = yes)
  tmi_fil1  — filter 1
  tmi_fil2  — filter 2
"""
from __future__ import annotations
from pathlib import Path
from etl.ingestion.readers import stream_records
from etl.cleaning.encoding import fix_record
from etl.cleaning.normalizer import parse_date


def build_lookup(indem_def_path: Path) -> dict[str, dict]:
    """
    Returns {tmi_cind: indemnite_dict} with Arabic labels fixed.
    """
    lookup: dict[str, dict] = {}

    for rec in stream_records(indem_def_path):
        rec = fix_record(rec)
        code = str(rec.get("tmi_cind") or "").strip()
        if not code:
            continue

        lookup[code] = {
            "indemnite_code":     code,
            "indemnite_label_fr": str(rec.get("tmi_libc") or "").strip() or None,
            "indemnite_label_fr_long": str(rec.get("tmi_libl") or "").strip() or None,
            "indemnite_label_ar": str(rec.get("tmi_liba") or "").strip() or None,
            "nature_flag":        str(rec.get("tmi_nat")  or "").strip() or None,
            "is_taxable":         str(rec.get("tmi_imp")  or "").strip() == "1",
            "is_cnr":             str(rec.get("tmi_cnr")  or "").strip() == "1",
            "zone":               str(rec.get("tmi_zon")  or "").strip() or None,
            "arg1":               _to_num(rec.get("tmi_arg1")),
            "arg2":               _to_num(rec.get("tmi_arg2")),
            "date_entry":         parse_date(rec.get("tmi_dpc")),
            "insurance_code":     str(rec.get("tmi_cins") or "").strip() or None,
        }

    return lookup


def match(code: str | None, lookup: dict[str, dict]) -> tuple[dict | None, str]:
    """Returns (indemnite_dict, method) or (None, 'no_match')."""
    if not code:
        return None, "no_code"
    c = str(code).strip()
    hit = lookup.get(c)
    return (hit, "exact") if hit else (None, "no_match")


def _to_num(v) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "."))
    except ValueError:
        return None
