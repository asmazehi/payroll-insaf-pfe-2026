"""
Region reference mapper.

Known limitation (documented)
──────────────────────────────
pa_loca is an opaque locality code that has no counterpart in region.json.
The locality sub-table was not exported from Oracle.

Strategy
────────
Match pa_codmin → coddep only.
  - If that resolves to exactly 1 region: match_codmin_unique
  - If multiple regions share the same coddep: ambiguous → Unknown
  - If no match: no_match → Unknown

Unknown region (region_sk = 0) is the correct and honest result
for the majority of rows until the locality table is recovered.
"""
from __future__ import annotations
from pathlib import Path
from etl.ingestion.readers import stream_records
from etl.cleaning.encoding import fix_record


def build_lookup(region_path: Path) -> dict:
    """
    Returns {
        "by_coddep_codreg": {(coddep, codreg): region_dict},
        "by_coddep":        {coddep: [region_dict, ...]},
    }
    """
    by_full: dict[tuple, dict] = {}
    by_dep:  dict[str, list]   = {}

    for rec in stream_records(region_path):
        rec = fix_record(rec)
        coddep  = str(rec.get("coddep")  or "").strip().upper() or None
        codreg  = str(rec.get("codreg")  or "").strip().upper() or None

        if not coddep:
            continue

        entry = {
            "coddep":       coddep,
            "codreg":       codreg,
            "lib_reg":      str(rec.get("lib_reg")  or "").strip() or None,
            "lib_rega":     str(rec.get("lib_rega") or "").strip() or None,
            "code_dept":    str(rec.get("code_dept")    or "").strip() or None,
            "code_region":  str(rec.get("code_region")  or "").strip() or None,
        }

        key = (coddep, codreg or "")
        if key not in by_full:
            by_full[key] = entry

        by_dep.setdefault(coddep, []).append(entry)

    return {"by_full": by_full, "by_dep": by_dep}


def match(record: dict, lookup: dict) -> tuple[dict | None, str]:
    """
    Returns (region_dict, method) or (None, reason).
    method values:
      'exact_coddep_codreg' | 'match_coddep_unique' | 'ambiguous' | 'no_match'
    """
    codmin = str(record.get("pa_codmin") or "").strip().upper()
    if not codmin:
        return None, "no_codmin"

    candidates = lookup["by_dep"].get(codmin, [])
    if not candidates:
        return None, "no_match"

    if len(candidates) == 1:
        return candidates[0], "match_coddep_unique"

    # Multiple regions — cannot disambiguate without pa_loca crosswalk
    return None, "ambiguous_multiple_regions"
