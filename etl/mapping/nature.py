"""Nature reference mapper — pa_natu → dim_nature."""
from __future__ import annotations
from pathlib import Path
from etl.ingestion.readers import stream_records
from etl.cleaning.encoding import fix_record


def build_lookup(nature_path: Path) -> dict[str, dict]:
    """
    Returns {nature_code: nature_dict} with Arabic labels fixed.
    Key = CODNAT (1-char code: '1'–'9', 'A'–'C').
    """
    lookup: dict[str, dict] = {}
    for rec in stream_records(nature_path):
        rec = fix_record(rec)
        code = str(rec.get("codnat") or "").strip().upper()
        if not code:
            continue
        lookup[code] = {
            "nature_code":     code,
            "nature_type":     str(rec.get("typnat") or "").strip() or None,
            "nature_label_fr": str(rec.get("libnatl") or "").strip() or None,
            "nature_label_ar": str(rec.get("libnata") or "").strip() or None,
        }
    return lookup


def match(pa_natu: str | None, lookup: dict[str, dict]) -> tuple[dict | None, str]:
    """Returns (nature_dict, method) or (None, 'no_match')."""
    if not pa_natu:
        return None, "no_code"
    code = str(pa_natu).strip().upper()
    hit = lookup.get(code)
    return (hit, "exact") if hit else (None, "no_match")
