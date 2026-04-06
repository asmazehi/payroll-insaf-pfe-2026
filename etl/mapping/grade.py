"""Grade reference mapper — pa_grd → dim_grade."""
from __future__ import annotations
from pathlib import Path
from etl.ingestion.readers import stream_records
from etl.cleaning.encoding import fix_record


def build_lookup(grade_path: Path) -> dict[str, dict]:
    """
    Returns {grade_code: grade_dict} with Arabic labels fixed.
    Key = CODGRD (3-char, uppercase).
    """
    lookup: dict[str, dict] = {}
    for rec in stream_records(grade_path):
        rec = fix_record(rec)
        code = str(rec.get("codgrd") or "").strip().upper()
        if not code:
            continue
        lookup[code] = {
            "grade_code":     code,
            "grade_label_fr": (rec.get("libcgrdl") or rec.get("liblgrdl") or "").strip() or None,
            "grade_label_ar": (rec.get("libcgrda") or rec.get("liblgrda") or "").strip() or None,
            "category":       str(rec.get("cat")      or "").strip() or None,
            "class_grade":    str(rec.get("classgrd") or "").strip() or None,
            "retire_age":     _to_int(rec.get("ageret")),
        }
    return lookup


def match(pa_grd: str | None, lookup: dict[str, dict]) -> tuple[dict | None, str]:
    """
    Returns (grade_dict, method) or (None, 'no_match').
    method values: 'exact' | 'no_match'
    """
    if not pa_grd:
        return None, "no_code"
    code = str(pa_grd).strip().upper()
    hit = lookup.get(code)
    return (hit, "exact") if hit else (None, "no_match")


def _to_int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
