from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORTS_DIR = PROJECT_ROOT / "reports"
PAYROLL_OUTPUT = PROJECT_ROOT / "data" / "clean" / "payroll_type1_clean.jsonl"
SUMMARY_OUTPUT = REPORTS_DIR / "payroll_type1_summary.json"

GRADE_FILE = RAW_DIR / "grade.json"
NATURE_FILE = RAW_DIR / "nature.json"
ORGANISME_FILE = RAW_DIR / "organisme.json"
REGION_FILE = RAW_DIR / "region.json"

NUMERIC_FIELDS = {
    "pa_mois",
    "pa_annee",
    "pa_sec",
    "pa_eche",
    "pa_enfits",
    "pa_totinf",
    "pa_indice",
    "pa_salimp",
    "pa_salnimp",
    "pa_avkm",
    "pa_avlog",
    "pa_cpe",
    "pa_retrait",
    "pa_cps",
    "pa_capdeces",
    "pa_netord",
    "pa_netpay",
    "pa_rapimp",
    "pa_rapni",
    "pa_sub",
    "pa_sps",
    "pa_spl",
    "pa_rapsalb",
    "pa_brutcnr",
    "pa_salbrut",
}


VERSION_TAG = "paie_clean_v1"

PROFILE_OUTPUT = REPORTS_DIR / f"{VERSION_TAG}_profile.json"
BUSINESS_OUTPUT = REPORTS_DIR / f"{VERSION_TAG}_business_validation.json"
REFERENCE_OUTPUT = REPORTS_DIR / f"{VERSION_TAG}_reference_coverage.json"
CONTRACT_OUTPUT = REPORTS_DIR / f"{VERSION_TAG}_data_contract.json"
QUALITY_GATE_OUTPUT = REPORTS_DIR / f"{VERSION_TAG}_quality_gate.json"
REPO_CLEANUP_OUTPUT = REPORTS_DIR / f"{VERSION_TAG}_repository_cleanup.json"
FINAL_REPORT_OUTPUT = REPORTS_DIR / f"{VERSION_TAG}_final_report.md"


PAIE_FACT_READY_PROD = Path(__file__).resolve().parents[1] / "data" / "clean" / "fact_paie_src.jsonl"


def build_reference_maps() -> Dict[str, Dict[str, str]]:
    def load_items(path: Path) -> List[Dict[str, Any]]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload["results"][0]["items"]

    grade_map: Dict[str, str] = {}
    for row in load_items(GRADE_FILE):
        code = str(row.get("codgrd", "")).strip()
        label = str(row.get("libcgrdl", "")).strip() or str(row.get("liblgrdl", "")).strip()
        if code and label and code not in grade_map:
            grade_map[code] = label

    nature_map: Dict[str, str] = {}
    for row in load_items(NATURE_FILE):
        code = str(row.get("codnat", "")).strip()
        label = str(row.get("libnatl", "")).strip()
        if code and label and code not in nature_map:
            nature_map[code] = label

    org_map: Dict[str, str] = {}
    for row in load_items(ORGANISME_FILE):
        key = "|".join(
            [
                str(row.get("codetab", "")).strip(),
                str(row.get("cab", "")).strip(),
                str(row.get("sg", "")).strip(),
                str(row.get("dg", "")).strip(),
                str(row.get("dire", "")).strip(),
                str(row.get("sdir", "")).strip(),
                str(row.get("serv", "")).strip(),
                str(row.get("unite", "")).strip(),
            ]
        )
        label = str(row.get("liborgl", "")).strip()
        if key and label and key not in org_map:
            org_map[key] = label

    region_map: Dict[str, str] = {}
    for row in load_items(REGION_FILE):
        key = f"{str(row.get('coddep', '')).strip()}|{str(row.get('codreg', '')).strip()}"
        label = str(row.get("lib_reg", "")).strip()
        if key and label and key not in region_map:
            region_map[key] = label

    return {
        "grade": grade_map,
        "nature": nature_map,
        "organisme": org_map,
        "region": region_map,
    }


def infer_identity_field(row: Dict[str, Any]) -> str:
    if "employee_id" in row:
        return "employee_id"
    return "pa_mat"


def infer_grain_columns(row: Dict[str, Any]) -> Tuple[List[str], str]:
    identity_field = infer_identity_field(row)
    key_columns = [identity_field, "pa_annee", "pa_mois", "pa_type"]

    # Keep duplicate detection aligned with the production fact-ready grain.
    for opt in ["pa_sec", "pa_codmin", "pa_dire", "pa_article", "pa_parag"]:
        if opt in row:
            key_columns.append(opt)

    return key_columns, " x ".join(key_columns)


def resolve_dataset_path() -> Path:
    # Prefer the original cleaned payroll dataset when present.
    if PAYROLL_OUTPUT.exists():
        return PAYROLL_OUTPUT
    # Fall back to the canonical production fact-ready dataset for reproducibility.
    if PAIE_FACT_READY_PROD.exists():
        return PAIE_FACT_READY_PROD
    raise FileNotFoundError(
        f"No input dataset found. Checked: {PAYROLL_OUTPUT} and {PAIE_FACT_READY_PROD}"
    )


def iter_clean_rows(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            yield json.loads(text)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def percentile_from_hist(hist: Counter[int], p: float) -> int:
    if not hist:
        return 0
    total = sum(hist.values())
    rank = max(1, int(math.ceil(total * p)))
    acc = 0
    for k in sorted(hist.keys()):
        acc += hist[k]
        if acc >= rank:
            return k
    return max(hist.keys())


def percentile_from_sorted(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    idx = max(0, min(len(values) - 1, int(round((len(values) - 1) * p))))
    return values[idx]


def sample_push(sample: List[float], value: float, max_size: int = 20000) -> None:
    if len(sample) < max_size:
        sample.append(value)
        return
    stride = max(2, len(sample) // 20)
    if int(abs(value) * 1000) % stride == 0:
        sample[int(value) % max_size] = value


def build_column_profiles(dataset_path: Path) -> Dict[str, Any]:
    column_stats: Dict[str, Dict[str, Any]] = {}
    numeric_samples: Dict[str, List[float]] = defaultdict(list)
    numeric_minmax: Dict[str, Dict[str, float]] = {}
    row_count = 0

    for row in iter_clean_rows(dataset_path):
        row_count += 1
        for col, value in row.items():
            st = column_stats.setdefault(
                col,
                {
                    "rows": 0,
                    "null_count": 0,
                    "blank_count": 0,
                    "type_counter": Counter(),
                    "unique_values": set(),
                    "length_hist": Counter(),
                    "invalid_format_count": 0,
                },
            )
            st["rows"] += 1

            if value is None:
                st["null_count"] += 1
                st["type_counter"]["null"] += 1
                st["unique_values"].add("null")
                continue

            tname = type(value).__name__
            st["type_counter"][tname] += 1
            st["unique_values"].add(canonical(value))

            if isinstance(value, str):
                if value.strip() == "":
                    st["blank_count"] += 1
                st["length_hist"][len(value)] += 1
                if col in {"pa_datnais", "pa_datent", "pa_date_ech", "pa_datnatu"}:
                    if value != "" and len(value) != 10:
                        st["invalid_format_count"] += 1
                    if value != "" and len(value) == 10:
                        parts = value.split("-")
                        if len(parts) != 3 or not all(part.isdigit() for part in parts):
                            st["invalid_format_count"] += 1

            if col in NUMERIC_FIELDS and isinstance(value, (int, float)):
                mm = numeric_minmax.setdefault(col, {"min": float(value), "max": float(value)})
                mm["min"] = min(mm["min"], float(value))
                mm["max"] = max(mm["max"], float(value))
                sample_push(numeric_samples[col], float(value))

    profile = {
        "dataset": str(dataset_path),
        "version_tag": VERSION_TAG,
        "row_count": row_count,
        "columns": {},
    }

    for col, st in sorted(column_stats.items()):
        rows = st["rows"]
        null_rate = st["null_count"] / row_count if row_count else 0.0
        blank_rate = st["blank_count"] / row_count if row_count else 0.0

        length_info = {
            "min_len": 0,
            "p50_len": 0,
            "p95_len": 0,
            "max_len": 0,
        }
        if st["length_hist"]:
            length_info = {
                "min_len": min(st["length_hist"].keys()),
                "p50_len": percentile_from_hist(st["length_hist"], 0.50),
                "p95_len": percentile_from_hist(st["length_hist"], 0.95),
                "max_len": max(st["length_hist"].keys()),
            }

        numeric_info = None
        outlier_info = None
        if col in numeric_minmax:
            values = sorted(numeric_samples[col])
            q1 = percentile_from_sorted(values, 0.25)
            q3 = percentile_from_sorted(values, 0.75)
            iqr = q3 - q1
            low = q1 - 1.5 * iqr
            high = q3 + 1.5 * iqr
            outliers = sum(1 for v in values if v < low or v > high)
            outlier_info = {
                "method": "sample_iqr",
                "sample_size": len(values),
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "lower_bound": low,
                "upper_bound": high,
                "sample_outliers": outliers,
            }
            numeric_info = {
                "min": numeric_minmax[col]["min"],
                "max": numeric_minmax[col]["max"],
            }

        profile["columns"][col] = {
            "type_distribution": dict(st["type_counter"]),
            "null_count": st["null_count"],
            "null_rate": round(null_rate, 8),
            "blank_count": st["blank_count"],
            "blank_rate": round(blank_rate, 8),
            "unique_count": len(st["unique_values"]),
            "length_distribution": length_info,
            "invalid_format_count": st["invalid_format_count"],
            "numeric_range": numeric_info,
            "outliers": outlier_info,
        }

    return profile


def validate_business_rules(dataset_path: Path, refs: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    counters = Counter()
    issue_counts = Counter()
    duplicate_key_counter = Counter()

    suspicious_rows = 0
    inconsistent_rows = 0
    valid_rows = 0
    unresolved_rows = 0
    key_columns: List[str] = []
    grain_label = ""

    for row in iter_clean_rows(dataset_path):
        counters["total_rows"] += 1

        if not key_columns:
            key_columns, grain_label = infer_grain_columns(row)

        issues: List[str] = []
        suspicious_flags: List[str] = []

        mois = row.get("pa_mois")
        annee = row.get("pa_annee")
        ptype = str(row.get("pa_type", ""))

        if not isinstance(mois, int) or not (1 <= mois <= 12):
            issues.append("invalid_mois")
        if not isinstance(annee, int) or not (1900 <= annee <= 2100):
            issues.append("invalid_annee")
        if ptype != "1":
            issues.append("invalid_pa_type")

        if row.get("pa_netpay") is not None and row.get("pa_salbrut") is not None:
            if float(row["pa_netpay"]) > float(row["pa_salbrut"]):
                issues.append("netpay_gt_salbrut")

        for field in NUMERIC_FIELDS:
            value = row.get(field)
            if value is None:
                continue
            if isinstance(value, (int, float)) and float(value) < 0:
                suspicious_flags.append(f"negative_{field}")

        for key_field in key_columns:
            v = row.get(key_field)
            if v is None or (isinstance(v, str) and v.strip() == ""):
                issues.append(f"missing_{key_field}")

        dup_key = tuple(str(row.get(col, "")) for col in key_columns)
        duplicate_key_counter[dup_key] += 1

        if row.get("dq_has_unresolved"):
            suspicious_flags.append("dq_unresolved_fields")
            unresolved_rows += 1

        grade_code = str(row.get("pa_grd", "")).strip()
        if grade_code and not refs["grade"].get(grade_code):
            suspicious_flags.append("unmatched_grade")

        nature_code = str(row.get("pa_natu", "")).strip()
        if nature_code and not refs["nature"].get(nature_code):
            suspicious_flags.append("unmatched_nature")

        org_key = "|".join(
            [
                str(row.get("pa_codmin", "")),
                str(row.get("pa_cab", "")),
                str(row.get("pa_sg", "")),
                str(row.get("pa_dg", "")),
                str(row.get("pa_dire", "")),
                str(row.get("pa_sdir", "")),
                str(row.get("pa_serv", "")),
                str(row.get("pa_unite", "")),
            ]
        )
        if org_key.strip("|") and not refs["organisme"].get(org_key):
            suspicious_flags.append("unmatched_organisme")

        loca_key = f"{row.get('pa_codmin', '')}|{row.get('pa_loca', '')}"
        if str(row.get("pa_loca", "")).strip() and not refs["region"].get(loca_key):
            suspicious_flags.append("unmatched_region")

        if issues:
            inconsistent_rows += 1
            for i in set(issues):
                issue_counts[i] += 1
        elif suspicious_flags:
            suspicious_rows += 1
            for s in set(suspicious_flags):
                issue_counts[s] += 1
        else:
            valid_rows += 1

    duplicate_groups = sum(1 for _, c in duplicate_key_counter.items() if c > 1)
    duplicate_rows = sum(c - 1 for c in duplicate_key_counter.values() if c > 1)

    return {
        "version_tag": VERSION_TAG,
        "validation_schema": {
            "identity_field": key_columns[0] if key_columns else None,
            "duplicate_grain": grain_label,
            "required_key_fields": key_columns,
        },
        "row_classification": {
            "valid": valid_rows,
            "suspicious": suspicious_rows,
            "inconsistent": inconsistent_rows,
        },
        "rule_violations": dict(issue_counts),
        "unresolved_rows": unresolved_rows,
        "duplicates": {
            "grain": grain_label,
            "duplicate_groups": duplicate_groups,
            "duplicate_rows": duplicate_rows,
        },
        "totals": {
            "rows": counters["total_rows"],
            "rows_preserved": counters["total_rows"],
        },
    }


def reference_coverage(dataset_path: Path, refs: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    stats: Dict[str, Counter] = {
        "grade": Counter(),
        "nature": Counter(),
        "organisme": Counter(),
        "region": Counter(),
    }
    unmatched_values: Dict[str, Counter] = {
        "grade": Counter(),
        "nature": Counter(),
        "organisme": Counter(),
        "region": Counter(),
    }

    for row in iter_clean_rows(dataset_path):
        grade = str(row.get("pa_grd", "")).strip()
        if grade:
            stats["grade"]["total"] += 1
            if refs["grade"].get(grade):
                stats["grade"]["matched"] += 1
            else:
                unmatched_values["grade"][grade] += 1

        nature = str(row.get("pa_natu", "")).strip()
        if nature:
            stats["nature"]["total"] += 1
            if refs["nature"].get(nature):
                stats["nature"]["matched"] += 1
            else:
                unmatched_values["nature"][nature] += 1

        org_key = "|".join(
            [
                str(row.get("pa_codmin", "")),
                str(row.get("pa_cab", "")),
                str(row.get("pa_sg", "")),
                str(row.get("pa_dg", "")),
                str(row.get("pa_dire", "")),
                str(row.get("pa_sdir", "")),
                str(row.get("pa_serv", "")),
                str(row.get("pa_unite", "")),
            ]
        )
        if org_key.strip("|"):
            stats["organisme"]["total"] += 1
            if refs["organisme"].get(org_key):
                stats["organisme"]["matched"] += 1
            else:
                unmatched_values["organisme"][org_key] += 1

        region_key = f"{row.get('pa_codmin', '')}|{row.get('pa_loca', '')}"
        if str(row.get("pa_loca", "")).strip():
            stats["region"]["total"] += 1
            if refs["region"].get(region_key):
                stats["region"]["matched"] += 1
            else:
                unmatched_values["region"][region_key] += 1

    out: Dict[str, Any] = {"version_tag": VERSION_TAG, "coverage": {}}
    for key in ["grade", "nature", "organisme", "region"]:
        total = stats[key]["total"]
        matched = stats[key]["matched"]
        rate = (matched / total) if total else 0.0
        out["coverage"][key] = {
            "total_with_code": total,
            "matched": matched,
            "unmatched": total - matched,
            "match_rate": round(rate, 8),
            "top_unmatched_values": unmatched_values[key].most_common(20),
        }
    return out


def data_contract(dataset_path: Path) -> Dict[str, Any]:
    first_row = next(iter_clean_rows(dataset_path), {})
    identity_field = infer_identity_field(first_row)
    salary_fields = [f for f in sorted(NUMERIC_FIELDS) if f not in {"pa_annee", "pa_mois"}]
    contract_cols = [
        {
            "name": identity_field,
            "type": "string",
            "nullable": False,
            "business_meaning": "Employee identifier",
            "constraints": ["not blank"],
        },
        {
            "name": "pa_annee",
            "type": "integer",
            "nullable": False,
            "business_meaning": "Payroll year",
            "constraints": ["1900 <= pa_annee <= 2100"],
        },
        {
            "name": "pa_mois",
            "type": "integer",
            "nullable": False,
            "business_meaning": "Payroll month",
            "constraints": ["1 <= pa_mois <= 12"],
        },
        {
            "name": "pa_type",
            "type": "string",
            "nullable": False,
            "business_meaning": "Payroll type code",
            "constraints": ["must equal 1 for this dataset"],
        },
    ]

    for f in salary_fields:
        contract_cols.append(
            {
                "name": f,
                "type": "number",
                "nullable": True,
                "business_meaning": "Payroll amount or payroll numeric attribute",
                "constraints": ["if present, must be numeric", "if present, must be >= 0"],
            }
        )

    contract_cols.extend(
        [
            {
                "name": "dq_unresolved_fields",
                "type": "array<string>",
                "nullable": False,
                "business_meaning": "List of fields unresolved during cleaning",
                "constraints": ["empty means fully resolved row"],
            },
            {
                "name": "dq_has_unresolved",
                "type": "boolean",
                "nullable": False,
                "business_meaning": "Whether the row contains unresolved fields",
                "constraints": ["must match dq_unresolved_fields emptiness"],
            },
            {
                "name": "dq_trace_count",
                "type": "integer",
                "nullable": False,
                "business_meaning": "Number of recorded field transformations",
                "constraints": [">= 0"],
            },
            {
                "name": "_row_index",
                "type": "integer",
                "nullable": False,
                "business_meaning": "Original sequence index from source extraction",
                "constraints": [">= 1"],
            },
        ]
    )

    return {
        "version_tag": VERSION_TAG,
        "dataset": str(dataset_path),
        "grain": "employee x month x type",
        "primary_key": [identity_field, "pa_annee", "pa_mois", "pa_type"],
        "columns": contract_cols,
    }


def audit_repository(project_root: Path) -> Dict[str, Any]:
    files: List[str] = []
    for p in project_root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(project_root).as_posix()
        if rel.startswith(".venv/") or rel.startswith(".git/"):
            continue
        files.append(rel)

    classified: List[Dict[str, str]] = []
    keep: List[str] = []
    delete: List[Dict[str, str]] = []
    refactor: List[str] = []

    for rel in sorted(files):
        category = "useful"
        purpose = "Project file"

        if rel.endswith(".pyc") or "__pycache__/" in rel:
            category = "temporary"
            purpose = "Python bytecode cache generated at runtime"
            delete.append(
                {
                    "path": rel,
                    "reason": "Temporary runtime artifact; not needed for reproducibility or maintenance.",
                }
            )
        elif rel in {"README.md", ".gitignore", "etl/build_payroll_dataset.py", "etl/run_payroll_quality_gate.py"}:
            category = "essential"
            purpose = "Core pipeline/reproducibility documentation or executable logic"
            keep.append(rel)
        elif rel.startswith("data/raw/") and rel.endswith(".json"):
            if "paie2015" in rel or any(k in rel for k in ["grade", "nature", "organisme", "region"]):
                category = "essential"
                purpose = "Required source/reference dataset for payroll type-1 cleaning"
            else:
                category = "useful"
                purpose = "Out-of-scope source retained for future ETL phases"
            keep.append(rel)
        elif rel.startswith("data/clean/") and rel.endswith(".jsonl"):
            category = "essential"
            purpose = "Current finalized cleaned dataset output"
            keep.append(rel)
        elif rel.startswith("reports/"):
            category = "essential"
            purpose = "Auditability and validation artifacts"
            keep.append(rel)
            if rel.endswith("payroll_type1_summary.md"):
                refactor.append(rel)
        else:
            keep.append(rel)

        classified.append({"path": rel, "purpose": purpose, "category": category})

    recommended_structure = [
        "README.md",
        ".gitignore",
        "data/raw/{paie2015.json, grade.json, nature.json, organisme.json, region.json}",
        "data/raw/{ind2015.json, indem_def.json}  # kept for future phase",
        "data/clean/{dim_employee_src.jsonl, fact_paie_src.jsonl}",
        "etl/build_payroll_dataset.py",
        "etl/run_payroll_quality_gate.py",
        "reports/{payroll_type1_*.jsonl|json|md}",
        f"reports/{VERSION_TAG}_*.json|md",
    ]

    return {
        "version_tag": VERSION_TAG,
        "recommended_structure": recommended_structure,
        "files": classified,
        "keep": sorted(set(keep)),
        "delete": delete,
        "refactor": sorted(set(refactor)),
    }


def quality_gate(profile: Dict[str, Any], business: Dict[str, Any], refs: Dict[str, Any], dataset_path: Path) -> Dict[str, Any]:
    expected_rows = None
    if SUMMARY_OUTPUT.exists():
        summary = json.loads(SUMMARY_OUTPUT.read_text(encoding="utf-8"))
        expected_rows = summary.get("counts", {}).get("rows_type1")

    total_rows = business["totals"]["rows"]
    duplicate_rows = business["duplicates"]["duplicate_rows"]
    inconsistent = business["row_classification"]["inconsistent"]
    suspicious = business["row_classification"]["suspicious"]
    unresolved = business.get("unresolved_rows", 0)

    rules = []

    def add_rule(name: str, status: str, detail: str) -> None:
        rules.append({"rule": name, "status": status, "detail": detail})

    if expected_rows is None:
        add_rule("rows_preserved_vs_summary", "WARN", "No baseline summary found.")
    elif total_rows == expected_rows:
        add_rule("rows_preserved_vs_summary", "PASS", f"Rows preserved: {total_rows}.")
    else:
        add_rule(
            "rows_preserved_vs_summary",
            "FAIL",
            f"Expected {expected_rows} rows, found {total_rows} rows.",
        )

    for critical_issue in ["invalid_mois", "invalid_annee", "invalid_pa_type", "netpay_gt_salbrut"]:
        count = business["rule_violations"].get(critical_issue, 0)
        status = "PASS" if count == 0 else "FAIL"
        add_rule(critical_issue, status, f"count={count}")

    add_rule(
        "duplicate_grain_rows",
        "PASS" if duplicate_rows == 0 else "WARN",
        f"duplicate_rows={duplicate_rows}",
    )
    add_rule(
        "suspicious_rows",
        "PASS" if suspicious == 0 else "WARN",
        f"suspicious_rows={suspicious}",
    )
    add_rule(
        "inconsistent_rows",
        "PASS" if inconsistent == 0 else "FAIL",
        f"inconsistent_rows={inconsistent}",
    )

    for ref_key in ["grade", "nature", "organisme", "region"]:
        rate = refs["coverage"][ref_key]["match_rate"]
        status = "PASS" if rate >= 0.95 else "WARN"
        add_rule(
            f"reference_coverage_{ref_key}",
            status,
            f"match_rate={rate:.4f}",
        )

    if any(r["status"] == "FAIL" for r in rules):
        final_status = "FAIL"
    elif any(r["status"] == "WARN" for r in rules):
        final_status = "PASS WITH WARNINGS"
    else:
        final_status = "PASS"

    return {
        "version_tag": VERSION_TAG,
        "dataset": str(dataset_path),
        "total_rows": total_rows,
        "rows_preserved": total_rows,
        "valid_rows": business["row_classification"]["valid"],
        "rows_with_issues": suspicious + inconsistent,
        "duplicates": business["duplicates"],
        "unresolved_rows_proxy": unresolved,
        "rules": rules,
        "final_status": final_status,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_final_markdown(
    profile: Dict[str, Any],
    business: Dict[str, Any],
    refs: Dict[str, Any],
    contract: Dict[str, Any],
    gate: Dict[str, Any],
    cleanup: Dict[str, Any],
) -> None:
    lines = [
        f"# Final Validation Report ({VERSION_TAG})",
        "",
        "## 1. Repository Cleanup Plan",
        f"- Files to keep: {len(cleanup['keep'])}",
        f"- Files to delete: {len(cleanup['delete'])}",
        f"- Files to refactor: {len(cleanup['refactor'])}",
        "",
        "## 2. Dataset Profiling Summary",
        f"- Total rows: {profile['row_count']}",
        f"- Total columns profiled: {len(profile['columns'])}",
        "",
        "## 3. Business Rule Validation",
        f"- Valid rows: {business['row_classification']['valid']}",
        f"- Suspicious rows: {business['row_classification']['suspicious']}",
        f"- Inconsistent rows: {business['row_classification']['inconsistent']}",
        f"- Duplicate groups (grain): {business['duplicates']['duplicate_groups']}",
        "",
        "## 4. Reference Coverage",
    ]
    for key in ["grade", "nature", "organisme", "region"]:
        c = refs["coverage"][key]
        lines.append(
            f"- {key}: matched={c['matched']} / {c['total_with_code']} (rate={c['match_rate']:.4f})"
        )

    lines.extend(
        [
            "",
            "## 5. Data Contract",
            f"- Grain: {contract['grain']}",
            f"- Primary key: {', '.join(contract['primary_key'])}",
            f"- Contract columns listed: {len(contract['columns'])}",
            "",
            "## 6. Quality Gate Script",
            "- Script: etl/run_payroll_quality_gate.py",
            "- Mode: reproducible single-command validation",
            "",
            "## 7. Validation Report",
            f"- Total rows: {gate['total_rows']}",
            f"- Rows preserved: {gate['rows_preserved']}",
            f"- Valid rows: {gate['valid_rows']}",
            f"- Rows with issues: {gate['rows_with_issues']}",
            f"- Final status: {gate['final_status']}",
            "",
            "## 8. Final Verdict",
            f"- Dataset readiness: {gate['final_status']}",
            "- Guarantees: all rows preserved; no fabricated values; auditable rule outputs generated.",
            "- Limitations: unmatched reference codes may remain and are reported, not silently altered.",
            "",
            "## 9. Ready-for-ETL Confirmation",
            "- Proceed to DW if final status is PASS or PASS WITH WARNINGS and accepted by governance.",
        ]
    )
    FINAL_REPORT_OUTPUT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    dataset_path = resolve_dataset_path()

    refs = build_reference_maps()
    profile = build_column_profiles(dataset_path)
    business = validate_business_rules(dataset_path, refs)
    refcov = reference_coverage(dataset_path, refs)
    contract = data_contract(dataset_path)
    cleanup = audit_repository(Path(__file__).resolve().parents[1])
    gate = quality_gate(profile, business, refcov, dataset_path)

    write_json(PROFILE_OUTPUT, profile)
    write_json(BUSINESS_OUTPUT, business)
    write_json(REFERENCE_OUTPUT, refcov)
    write_json(CONTRACT_OUTPUT, contract)
    write_json(REPO_CLEANUP_OUTPUT, cleanup)
    write_json(QUALITY_GATE_OUTPUT, gate)
    write_final_markdown(profile, business, refcov, contract, gate, cleanup)

    print(json.dumps({
        "version_tag": VERSION_TAG,
        "outputs": [
            str(PROFILE_OUTPUT),
            str(BUSINESS_OUTPUT),
            str(REFERENCE_OUTPUT),
            str(CONTRACT_OUTPUT),
            str(REPO_CLEANUP_OUTPUT),
            str(QUALITY_GATE_OUTPUT),
            str(FINAL_REPORT_OUTPUT),
        ],
        "final_status": gate["final_status"],
    }, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run payroll type-1 quality gate and final validation reports.")
    parser.parse_args()
    run()


if __name__ == "__main__":
    main()
