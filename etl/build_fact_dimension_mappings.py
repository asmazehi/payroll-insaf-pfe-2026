from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
REPORTS_DIR = PROJECT_ROOT / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"

FACT_FILE = CLEAN_DIR / "fact_paie_src.jsonl"
DIM_REGION_FILE = CLEAN_DIR / "dim_region_production.jsonl"
DIM_ORGANISME_FILE = CLEAN_DIR / "dim_organisme_src.jsonl"

REGION_BRIDGE_FILE = CLEAN_DIR / "map_region.jsonl"
ORGANISME_BRIDGE_FILE = CLEAN_DIR / "map_organisme.jsonl"
DIM_REGION_DW_FILE = CLEAN_DIR / "dim_region_src.jsonl"

REPORT_FILE = REPORTS_DIR / "paie_dw_mapping_layer_report.json"
TECH_NOTE_FILE = DOCS_DIR / "technical_notes.md"


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if text:
                yield json.loads(text)


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def load_region_lookups() -> Tuple[
    Dict[Tuple[str, str], Dict[str, Any]],
    Dict[Tuple[str, str], Dict[str, Any]],
    Dict[Tuple[str, str], Dict[str, Any]],
    Dict[Tuple[str, str], Dict[str, Any]],
]:
    by_codreg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    by_code_region: Dict[Tuple[str, str], Dict[str, Any]] = {}
    by_coddep: Dict[Tuple[str, str], Dict[str, Any]] = {}
    by_code_dept: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for row in iter_jsonl(DIM_REGION_FILE):
        coddep = norm(row.get("coddep"))
        codreg = norm(row.get("codreg"))
        code_region = norm(row.get("code_region"))
        code_dept = norm(row.get("code_dept"))

        if coddep and codreg:
            by_codreg[(coddep, codreg)] = row

        if coddep and coddep:
            by_coddep[(coddep, coddep)] = row

        if coddep and code_region:
            by_code_region[(coddep, code_region)] = row

        if coddep and code_dept:
            by_code_dept[(coddep, code_dept)] = row

    return by_codreg, by_code_region, by_coddep, by_code_dept


def map_region(
    pa_codmin: str,
    pa_loca: str,
    by_codreg: Dict[Tuple[str, str], Dict[str, Any]],
    by_code_region: Dict[Tuple[str, str], Dict[str, Any]],
    by_coddep: Dict[Tuple[str, str], Dict[str, Any]],
    by_code_dept: Dict[Tuple[str, str], Dict[str, Any]],
) -> Tuple[Optional[int], str]:
    if not pa_loca:
        return None, "no_region_code"

    row = by_codreg.get((pa_codmin, pa_loca))
    if row is not None:
        return int(row["region_key"]), "exact_coddep_codreg"

    row = by_code_region.get((pa_codmin, pa_loca))
    if row is not None:
        return int(row["region_key"]), "exact_coddep_code_region"

    row = by_coddep.get((pa_codmin, pa_loca))
    if row is not None:
        return int(row["region_key"]), "exact_coddep_coddep"

    row = by_code_dept.get((pa_codmin, pa_loca))
    if row is not None:
        return int(row["region_key"]), "exact_coddep_code_dept"

    return None, "unmapped"


def analyze_pa_loca_and_overlaps() -> Dict[str, Any]:
    region_rows = list(iter_jsonl(DIM_REGION_FILE))
    org_rows = list(iter_jsonl(DIM_ORGANISME_FILE))

    loca_counter = Counter()
    with FACT_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            loca = norm(row.get("pa_loca"))
            if loca:
                loca_counter[loca] += 1

    loca_set = set(loca_counter)
    total_rows_with_loca = sum(loca_counter.values())

    region_sets = {
        "dim_region.codreg": {norm(r.get("codreg")) for r in region_rows if norm(r.get("codreg"))},
        "dim_region.code_region": {norm(r.get("code_region")) for r in region_rows if norm(r.get("code_region"))},
        "dim_region.coddep": {norm(r.get("coddep")) for r in region_rows if norm(r.get("coddep"))},
        "dim_region.code_dept": {norm(r.get("code_dept")) for r in region_rows if norm(r.get("code_dept"))},
    }
    org_sets = {
        "dim_organisme.codloc": {
            norm(r.get("codloc"))
            for r in org_rows
            if norm(r.get("codloc")) and norm(r.get("codloc")) != "None"
        },
        "dim_organisme.codgouv": {
            norm(r.get("codgouv"))
            for r in org_rows
            if norm(r.get("codgouv")) and norm(r.get("codgouv")) != "None"
        },
        "dim_organisme.centreg": {
            norm(r.get("centreg"))
            for r in org_rows
            if norm(r.get("centreg")) and norm(r.get("centreg")) != "None"
        },
    }

    overlap_report: Dict[str, Dict[str, Any]] = {}
    for name, values in {**region_sets, **org_sets}.items():
        inter = loca_set & values
        overlap_report[name] = {
            "distinct_overlap": len(inter),
            "row_overlap": sum(loca_counter[v] for v in inter),
            "sample_values": sorted(inter)[:20],
        }

    pattern_rows = {
        "numeric_only": sum(c for v, c in loca_counter.items() if v.isdigit()),
        "alphanumeric_len3": sum(
            c
            for v, c in loca_counter.items()
            if len(v) == 3 and all(ch.isdigit() or ("A" <= ch <= "Z") for ch in v)
        ),
        "contains_pipe": sum(c for v, c in loca_counter.items() if "|" in v),
    }

    # Hypothesis test required by governance: pa_loca equals organisme.codloc.
    org_exact = {
        "|".join(norm(r.get(k)) for k in ["codetab", "cab", "sg", "dg", "dire", "sdir", "serv", "unite"]): r
        for r in org_rows
    }
    exact_org_rows = 0
    equal_non_empty = 0
    for row in iter_jsonl(FACT_FILE):
        okey = "|".join(
            norm(row.get(k))
            for k in ["pa_codmin", "pa_cab", "pa_sg", "pa_dg", "pa_dire", "pa_sdir", "pa_serv", "pa_unite"]
        )
        org_row = org_exact.get(okey)
        if org_row is None:
            continue
        exact_org_rows += 1
        pa_loca = norm(row.get("pa_loca"))
        codloc = norm(org_row.get("codloc"))
        if pa_loca and codloc and pa_loca == codloc:
            equal_non_empty += 1

    return {
        "distinct_pa_loca": len(loca_set),
        "rows_with_pa_loca": total_rows_with_loca,
        "top_pa_loca": loca_counter.most_common(30),
        "pattern_rows": pattern_rows,
        "overlaps": overlap_report,
        "hypothesis_pa_loca_equals_codloc": {
            "exact_organisme_rows_checked": exact_org_rows,
            "equal_non_empty_rows": equal_non_empty,
            "supported": equal_non_empty > 0,
        },
    }


def build_dw_region_dimension_with_unknown() -> Dict[str, Any]:
    base_rows = list(iter_jsonl(DIM_REGION_FILE))

    # Keep source dimension untouched and create a DW-only variant with explicit Unknown member.
    unknown_row = {
        "region_key": 0,
        "coddep": None,
        "code_dept": None,
        "code_region": None,
        "codreg": None,
        "codsreg": None,
        "fichier": None,
        "lib_reg": "Unknown",
        "lib_rega": "Unknown",
        "region_status": "UNKNOWN",
    }

    out_rows: List[Dict[str, Any]] = [unknown_row]
    for row in base_rows:
        copied = dict(row)
        copied["region_status"] = "MATCHED"
        out_rows.append(copied)

    written = write_jsonl(DIM_REGION_DW_FILE, out_rows)
    return {
        "source_dim_region": str(DIM_REGION_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "dw_dim_region": str(DIM_REGION_DW_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "unknown_region_key": 0,
        "unknown_label": "Unknown",
        "source_rows": len(base_rows),
        "dw_rows": written,
    }


def load_organisme_lookups() -> Tuple[Dict[str, Dict[str, Any]], Dict[Tuple[str, str], List[Dict[str, Any]]]]:
    fields = ["codetab", "cab", "sg", "dg", "dire", "sdir", "serv", "unite"]
    by_exact: Dict[str, Dict[str, Any]] = {}
    by_codetab_dire: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    for row in iter_jsonl(DIM_ORGANISME_FILE):
        key = "|".join(norm(row.get(k)) for k in fields)
        by_exact[key] = row

        cd = norm(row.get("codetab"))
        dr = norm(row.get("dire"))
        by_codetab_dire.setdefault((cd, dr), []).append(row)

    return by_exact, by_codetab_dire


def normalize_org_tokens(tokens: List[str]) -> List[str]:
    out = tokens[:]

    # Fill structural blanks with canonical "000" where payroll data often leaves empties.
    for idx in [1, 2, 3, 5, 6, 7]:
        if out[idx] == "":
            out[idx] = "000"

    # If direction is blank, align it with codetab (observed in source patterns).
    if out[4] == "" and out[0] != "":
        out[4] = out[0]

    return out


def map_organisme(
    tokens: List[str],
    by_exact: Dict[str, Dict[str, Any]],
    by_codetab_dire: Dict[Tuple[str, str], List[Dict[str, Any]]],
) -> Tuple[Optional[int], str, str]:
    raw_key = "|".join(tokens)
    if raw_key.strip("|") == "":
        return None, "no_organisme_code", raw_key

    row = by_exact.get(raw_key)
    if row is not None:
        return int(row["organisme_key"]), "exact_composite", raw_key

    normalized_tokens = normalize_org_tokens(tokens)
    normalized_key = "|".join(normalized_tokens)
    row = by_exact.get(normalized_key)
    if row is not None:
        return int(row["organisme_key"]), "normalized_fill_000", normalized_key

    cands = by_codetab_dire.get((normalized_tokens[0], normalized_tokens[4]), [])
    if len(cands) == 1:
        return int(cands[0]["organisme_key"]), "fallback_unique_codetab_dire", normalized_key

    return None, "unmapped", normalized_key


def main() -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    semantic_analysis = analyze_pa_loca_and_overlaps()
    region_dw_info = build_dw_region_dimension_with_unknown()

    by_codreg, by_code_region, by_coddep, by_code_dept = load_region_lookups()
    org_by_exact, org_by_codetab_dire = load_organisme_lookups()

    region_bridge: Dict[Tuple[str, str], Dict[str, Any]] = {}
    organisme_bridge: Dict[Tuple[str, str, str, str, str, str, str, str], Dict[str, Any]] = {}

    counters = Counter()
    region_method_counts = Counter()
    organisme_method_counts = Counter()

    for row in iter_jsonl(FACT_FILE):
        counters["fact_rows"] += 1

        pa_codmin = norm(row.get("pa_codmin"))
        pa_loca = norm(row.get("pa_loca"))

        if pa_loca:
            counters["region_rows_with_code"] += 1

            # Strict baseline for before coverage (legacy direct join).
            if (pa_codmin, pa_loca) in by_codreg:
                counters["region_before_matched"] += 1

            key = (pa_codmin, pa_loca)
            if key not in region_bridge:
                mapped_key, method = map_region(pa_codmin, pa_loca, by_codreg, by_code_region, by_coddep, by_code_dept)
                region_bridge[key] = {
                    "fact_pa_codmin": pa_codmin,
                    "fact_pa_loca": pa_loca,
                    "fact_region_code": f"{pa_codmin}|{pa_loca}",
                    "strict_region_key": mapped_key,
                    "mapped_region_key": mapped_key,
                    "dw_region_key": mapped_key if mapped_key is not None else 0,
                    "mapping_status": "MATCHED" if mapped_key is not None else "UNKNOWN",
                    "mapping_method": method,
                    "is_mapped": mapped_key is not None,
                    "is_unknown": mapped_key is None,
                }

            if region_bridge[key]["is_mapped"]:
                counters["region_after_matched"] += 1
            region_method_counts[region_bridge[key]["mapping_method"]] += 1

        org_tokens = [
            pa_codmin,
            norm(row.get("pa_cab")),
            norm(row.get("pa_sg")),
            norm(row.get("pa_dg")),
            norm(row.get("pa_dire")),
            norm(row.get("pa_sdir")),
            norm(row.get("pa_serv")),
            norm(row.get("pa_unite")),
        ]
        org_raw_key = "|".join(org_tokens)
        if org_raw_key.strip("|"):
            counters["organisme_rows_with_code"] += 1

            if org_raw_key in org_by_exact:
                counters["organisme_before_matched"] += 1

            org_key_tuple = tuple(org_tokens)
            if org_key_tuple not in organisme_bridge:
                mapped_key, method, normalized_key = map_organisme(org_tokens, org_by_exact, org_by_codetab_dire)
                organisme_bridge[org_key_tuple] = {
                    "fact_organisme_code_raw": org_raw_key,
                    "fact_pa_codmin": org_tokens[0],
                    "fact_pa_cab": org_tokens[1],
                    "fact_pa_sg": org_tokens[2],
                    "fact_pa_dg": org_tokens[3],
                    "fact_pa_dire": org_tokens[4],
                    "fact_pa_sdir": org_tokens[5],
                    "fact_pa_serv": org_tokens[6],
                    "fact_pa_unite": org_tokens[7],
                    "normalized_organisme_code": normalized_key,
                    "mapped_organisme_key": mapped_key,
                    "mapping_method": method,
                    "is_mapped": mapped_key is not None,
                }

            if organisme_bridge[org_key_tuple]["is_mapped"]:
                counters["organisme_after_matched"] += 1
            organisme_method_counts[organisme_bridge[org_key_tuple]["mapping_method"]] += 1

    region_bridge_rows = sorted(region_bridge.values(), key=lambda r: (r["fact_pa_codmin"], r["fact_pa_loca"]))
    organisme_bridge_rows = sorted(organisme_bridge.values(), key=lambda r: r["fact_organisme_code_raw"])

    region_bridge_count = write_jsonl(REGION_BRIDGE_FILE, region_bridge_rows)
    organisme_bridge_count = write_jsonl(ORGANISME_BRIDGE_FILE, organisme_bridge_rows)

    region_total = counters["region_rows_with_code"]
    region_before = counters["region_before_matched"]
    region_after = counters["region_after_matched"]

    org_total = counters["organisme_rows_with_code"]
    org_before = counters["organisme_before_matched"]
    org_after = counters["organisme_after_matched"]

    report = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "scope": "paie_dw_mapping_layer",
        "region_semantic_analysis": semantic_analysis,
        "inputs": {
            "fact": str(FACT_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "dim_region": str(DIM_REGION_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "dim_organisme": str(DIM_ORGANISME_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        },
        "outputs": {
            "region_bridge": str(REGION_BRIDGE_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "organisme_bridge": str(ORGANISME_BRIDGE_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "dim_region_dw": str(DIM_REGION_DW_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "report": str(REPORT_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "technical_note": str(TECH_NOTE_FILE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        },
        "dw_region_dimension_design": region_dw_info,
        "dw_fact_region_loading_rule": {
            "strict_match": "Use strict_region_key when mapping_status = MATCHED.",
            "unknown_handling": "Use dw_region_key = 0 when mapping_status = UNKNOWN.",
            "semantic_guardrail": "No codreg=000 fallback allowed.",
        },
        "coverage": {
            "region": {
                "before": {
                    "matched": region_before,
                    "total_with_code": region_total,
                    "match_rate": (region_before / region_total) if region_total else 0.0,
                },
                "after": {
                    "matched": region_after,
                    "total_with_code": region_total,
                    "match_rate": (region_after / region_total) if region_total else 0.0,
                },
                "unknown": {
                    "rows": region_total - region_after,
                    "rate": ((region_total - region_after) / region_total) if region_total else 0.0,
                },
                "methods_row_counts": dict(region_method_counts),
                "distinct_bridge_rows": region_bridge_count,
                "target_met_over_90": (region_after / region_total) > 0.90 if region_total else False,
                "mapping_is_semantic": True,
                "fallback_000_used": False,
            },
            "organisme": {
                "before": {
                    "matched": org_before,
                    "total_with_code": org_total,
                    "match_rate": (org_before / org_total) if org_total else 0.0,
                },
                "after": {
                    "matched": org_after,
                    "total_with_code": org_total,
                    "match_rate": (org_after / org_total) if org_total else 0.0,
                },
                "unknown": {
                    "rows": org_total - org_after,
                    "rate": ((org_total - org_after) / org_total) if org_total else 0.0,
                },
                "methods_row_counts": dict(organisme_method_counts),
                "distinct_bridge_rows": organisme_bridge_count,
                "target_met_over_80": (org_after / org_total) > 0.80 if org_total else False,
            },
        },
        "rules": {
            "no_source_data_modified": True,
            "no_rows_deleted": True,
            "no_values_fabricated": True,
            "mapping_only": True,
        },
    }

    REPORT_FILE.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    strict_rate = (region_after / region_total) if region_total else 0.0
    note_lines = [
        "# Region Mapping Technical Note",
        "",
        "- pa_loca behaves as an opaque locality code family (mostly 3-char alphanumeric tokens).",
        "- No valid crosswalk was found for most pa_loca codes against dim_region or organisme location fields.",
        f"- Strict region mapping coverage is {strict_rate:.6%} ({region_after}/{region_total}).",
        "- codreg=000 fallback mapping was rejected as semantically invalid.",
        "- Therefore, unmatched rows are treated as Unknown region (dw_region_key=0) in DW-safe outputs.",
    ]
    TECH_NOTE_FILE.write_text("\n".join(note_lines) + "\n", encoding="utf-8")

    print("Mapping layer built")
    print(f"Region bridge rows: {region_bridge_count}")
    print(f"Organisme bridge rows: {organisme_bridge_count}")
    print(
        "Region coverage before/after: "
        f"{region_before}/{region_total} -> {region_after}/{region_total}"
    )
    print(
        "Organisme coverage before/after: "
        f"{org_before}/{org_total} -> {org_after}/{org_total}"
    )
    print(f"Report: {REPORT_FILE}")


if __name__ == "__main__":
    main()
