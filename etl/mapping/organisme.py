"""
Organisme reference mapper.

Join strategy
─────────────
The full 8-level composite key (codetab, cab, sg, dg, dire, sdir, serv, unite)
almost never matches because payroll records only carry a subset of those levels.

We use a tiered approach:
  1. Exact match on (codetab, cab, sg, dg, dire, sdir, serv, unite)
  2. Match on (codetab, dire)         — minimum viable key
  3. Fallback: codetab only, if that resolves to exactly 1 org

We NEVER fill blank codes with "000" — that fabricates data.
"""
from __future__ import annotations
from pathlib import Path
from etl.ingestion.readers import stream_records
from etl.cleaning.encoding import fix_record


def build_lookup(organisme_path: Path) -> dict:
    """
    Build three lookup dicts from the organisme reference:
      full_key  → (codetab, cab, sg, dg, dire, sdir, serv, unite)
      min_key   → (codetab, dire)
      by_codetab → codetab  → list of all orgs with that codetab
    """
    full: dict[tuple, dict] = {}
    mini: dict[tuple, dict] = {}
    by_ct: dict[str, list]  = {}

    for rec in stream_records(organisme_path):
        rec = fix_record(rec)

        ct    = _c(rec, "codetab", 3)
        cab   = _c(rec, "cab",   3)
        sg    = _c(rec, "sg",    3)
        dg    = _c(rec, "dg",    3)
        dire  = _c(rec, "dire",  3)
        sdir  = _c(rec, "sdir",  3)
        serv  = _c(rec, "serv",  3)
        unite = _c(rec, "unite", 3)

        if not ct:
            continue

        entry = {
            "codetab":  ct,
            "cab":      cab,
            "sg":       sg,
            "dg":       dg,
            "dire":     dire,
            "sdir":     sdir,
            "serv":     serv,
            "unite":    unite,
            "liborgl":  str(rec.get("liborgl") or "").strip() or None,
            "liborga":  str(rec.get("liborga") or "").strip() or None,
            "codgouv":  str(rec.get("codgouv") or "").strip() or None,
            "deleg":    str(rec.get("deleg")   or "").strip() or None,
            "typstruct":str(rec.get("typstruct") or "").strip() or None,
        }

        fk = (ct, cab, sg, dg, dire, sdir, serv, unite)
        if fk not in full:
            full[fk] = entry

        mk = (ct, dire)
        if mk not in mini:
            mini[mk] = entry

        by_ct.setdefault(ct, []).append(entry)

    return {"full": full, "mini": mini, "by_ct": by_ct}


def match(record: dict, lookup: dict) -> tuple[dict | None, str]:
    """
    Match a payroll record to an organisme.
    Returns (organisme_dict, method_str) or (None, 'no_match').
    """
    ct    = _f(record, "pa_codmin", 3)
    cab   = _f(record, "pa_cab",   3)
    sg    = _f(record, "pa_sg",    3)
    dg    = _f(record, "pa_dg",    3)
    dire  = _f(record, "pa_dire",  3)
    sdir  = _f(record, "pa_sdir",  3)
    serv  = _f(record, "pa_serv",  3)
    unite = _f(record, "pa_unite", 3)

    if not ct:
        return None, "no_codmin"

    # 1. Full 8-key match
    fk = (ct, cab, sg, dg, dire, sdir, serv, unite)
    hit = lookup["full"].get(fk)
    if hit:
        return hit, "exact_full_key"

    # 2. Minimum key (codetab + dire)
    if dire:
        mk = (ct, dire)
        hit = lookup["mini"].get(mk)
        if hit:
            return hit, "match_codmin_dire"

    # 3. Codetab-only if unique
    candidates = lookup["by_ct"].get(ct, [])
    if len(candidates) == 1:
        return candidates[0], "fallback_codetab_unique"

    return None, "no_match"


# ── helpers ───────────────────────────────────────────────────────────────────

def _c(rec: dict, field: str, pad: int) -> str | None:
    """Read a field from a reference record, strip & pad."""
    v = rec.get(field)
    if v is None:
        return None
    s = str(v).strip().upper()
    return s if s else None


def _f(rec: dict, field: str, pad: int) -> str | None:
    """Read a field from a fact record (already normalized)."""
    v = rec.get(field)
    if v is None:
        return None
    s = str(v).strip().upper()
    return s if s else None
