from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
REPORTS_DIR = PROJECT_ROOT / "reports"

PAIE_CLEAN = CLEAN_DIR / "payroll_type1_clean.jsonl"
DIM_EMPLOYEE_OUTPUT = CLEAN_DIR / "dim_employee.jsonl"
PAIE_FACT_READY_OUTPUT = CLEAN_DIR / "paie_fact_ready.jsonl"
BUILD_REPORT_OUTPUT = REPORTS_DIR / "dim_employee_build_report.json"

EMPLOYEE_ATTRIBUTES = {
    "pa_noml": "last_name",
    "pa_prenl": "first_name",
    "pa_sexe": "gender",
    "pa_datnais": "birth_date",
    "pa_datent": "hire_date",
}

SALARY_FIELDS = {
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

REFERENCE_FIELDS = {
    "pa_codmin",
    "pa_type",
    "pa_regcnr",
    "pa_capd",
    "pa_cab",
    "pa_sg",
    "pa_dg",
    "pa_dire",
    "pa_sdir",
    "pa_serv",
    "pa_unite",
    "pa_loca",
    "pa_article",
    "pa_parag",
    "pa_mp",
    "pa_grd",
    "pa_nbrfam",
    "pa_codconj",
    "pa_sitfam",
    "pa_efonc",
    "pa_fonc",
    "pa_natu",
    "pa_mutuel",
    "pa_typarmee",
}

REFERENCE_ENRICHMENT_FIELDS = {
    "ref_grade_label",
    "ref_nature_label",
    "ref_organisme_label",
    "ref_region_label",
}

DQ_FIELDS = {
    "dq_unresolved_fields",
    "dq_has_unresolved",
    "dq_trace_count",
    "_row_index",
}


def iter_paie_rows(path: Path) -> Iterator[Dict[str, Any]]:
    """Stream cleaned paie rows."""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            yield json.loads(text)


def resolve_inconsistency(
    values: List[Tuple[Any, int, int]],
) -> Tuple[Optional[Any], str]:
    """
    Resolve employee attribute inconsistency.

    values: list of (value, count, max_row_index)
    Returns: (resolved_value, resolution_method)

    Strategy:
    1. Most frequent non-null value
    2. If tie, most recent valid value (highest row_index)
    3. If all null/blank, return None
    """
    if not values:
        return None, "all_missing"

    filtered = [
        (v, cnt, idx)
        for v, cnt, idx in values
        if v is not None and (not isinstance(v, str) or v.strip() != "")
    ]

    if not filtered:
        return None, "all_missing"

    if len(filtered) == 1:
        return filtered[0][0], "single_non_null_value"

    sorted_by_freq = sorted(filtered, key=lambda x: (-x[1], -x[2]))
    most_freq_value = sorted_by_freq[0][0]
    most_freq_count = sorted_by_freq[0][1]

    if sorted_by_freq[1][1] == most_freq_count:
        return sorted_by_freq[0][0], "tie_resolved_by_recency"
    return most_freq_value, "most_frequent_value"


def build_dim_employee() -> Tuple[int, Dict[str, Any]]:
    """
    Build dim_employee from paie dataset.

    Returns: (total_employees, inconsistency_report)
    """
    employee_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {k: [] for k in EMPLOYEE_ATTRIBUTES})
    paie_total_rows = 0

    for row in iter_paie_rows(PAIE_CLEAN):
        paie_total_rows += 1
        pa_mat = row.get("pa_mat")
        if not pa_mat:
            continue

        for source_field, target_field in EMPLOYEE_ATTRIBUTES.items():
            value = row.get(source_field)
            row_idx = row.get("_row_index", 0)
            employee_data[pa_mat][source_field].append((value, row_idx))

    inconsistency_report = {
        "paie_total_rows": paie_total_rows,
        "unique_employees": len(employee_data),
        "employee_attribute_inconsistencies": {},
    }

    dim_records = []
    employee_key = 1

    for pa_mat in sorted(employee_data.keys()):
        attributes = employee_data[pa_mat]
        record = {
            "employee_key": employee_key,
            "employee_id": pa_mat,
        }

        for source_field, target_field in EMPLOYEE_ATTRIBUTES.items():
            values_with_idx = attributes[source_field]

            if not values_with_idx:
                record[target_field] = None
                record[f"{target_field}_resolution"] = "all_missing"
                continue

            value_freq: Dict[Any, int] = Counter()
            value_latest_idx: Dict[Any, int] = {}

            for value, row_idx in values_with_idx:
                value_freq[value] += 1
                value_latest_idx[value] = max(value_latest_idx.get(value, 0), row_idx)

            resolve_values = [
                (v, cnt, value_latest_idx[v]) for v, cnt in value_freq.items()
            ]

            resolved_value, resolution_method = resolve_inconsistency(resolve_values)

            record[target_field] = resolved_value
            record[f"{target_field}_resolution"] = resolution_method

            if len(resolve_values) > 1 and resolution_method != "single_non_null_value":
                if source_field not in inconsistency_report["employee_attribute_inconsistencies"]:
                    inconsistency_report["employee_attribute_inconsistencies"][
                        source_field
                    ] = []
                inconsistency_report["employee_attribute_inconsistencies"][source_field].append(
                    {
                        "employee_id": pa_mat,
                        "unique_values": len(resolve_values),
                        "resolution_method": resolution_method,
                        "resolved_value": resolved_value,
                    }
                )

        dim_records.append(record)
        employee_key += 1

    with DIM_EMPLOYEE_OUTPUT.open("w", encoding="utf-8") as f:
        for record in dim_records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    inconsistency_summary = {}
    for field, conflicts in inconsistency_report["employee_attribute_inconsistencies"].items():
        inconsistency_summary[field] = {
            "employees_with_conflicts": len(conflicts),
            "examples": conflicts[:5],
        }
    inconsistency_report["inconsistency_summary"] = inconsistency_summary

    return len(dim_records), inconsistency_report


def build_paie_fact_ready(
    dim_employee_records: int,
) -> Tuple[int, Dict[str, Any]]:
    """
    Build paie_fact_ready dataset.

    Steps:
    - Keep ALL rows
    - Remove employee descriptive columns
    - Keep pa_mat for temporary join
    - Keep all salary/reference/DQ fields
    """
    employee_key_map: Dict[str, int] = {}

    with DIM_EMPLOYEE_OUTPUT.open("r", encoding="utf-8") as f:
        for line in f:
            emp_rec = json.loads(line)
            employee_key_map[emp_rec["employee_id"]] = emp_rec["employee_key"]

    fact_records = []
    unmapped_rows = 0
    rows_written = 0

    for paie_row in iter_paie_rows(PAIE_CLEAN):
        pa_mat = paie_row.get("pa_mat")

        if pa_mat not in employee_key_map:
            unmapped_rows += 1

        fact_row = {
            "employee_key": employee_key_map.get(pa_mat),
            "employee_id": pa_mat,
        }

        for field in (
            SALARY_FIELDS
            | REFERENCE_FIELDS
            | REFERENCE_ENRICHMENT_FIELDS
            | DQ_FIELDS
        ):
            if field in paie_row:
                fact_row[field] = paie_row[field]

        fact_records.append(fact_row)
        rows_written += 1

    with PAIE_FACT_READY_OUTPUT.open("w", encoding="utf-8") as f:
        for record in fact_records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return rows_written, {"total_rows_written": rows_written, "unmapped_rows": unmapped_rows}


def validate_consistency(
    paie_total_rows: int,
    dim_employee_count: int,
    fact_ready_count: int,
) -> Dict[str, Any]:
    """Validate data consistency across datasets."""
    checks = {
        "paie_rows_preserved": paie_total_rows == fact_ready_count,
        "paie_total_rows": paie_total_rows,
        "fact_ready_rows": fact_ready_count,
        "dim_employee_count": dim_employee_count,
        "detail": {},
    }

    if paie_total_rows == fact_ready_count:
        checks["detail"]["row_preservation"] = "PASS — all paie rows preserved in fact_ready"
    else:
        checks["detail"]["row_preservation"] = f"FAIL — expected {paie_total_rows}, got {fact_ready_count}"

    checks["detail"]["employee_mapping"] = (
        f"dim_employee contains {dim_employee_count} unique employees"
    )

    return checks


def run_build() -> None:
    """Orchestrate full build process."""
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("PHASE 1: Building dim_employee from paie")
    print("=" * 80)
    dim_employee_count, inconsistency_report = build_dim_employee()
    print(f"✓ dim_employee built: {dim_employee_count} employees")
    print(f"  Inconsistency details: {inconsistency_report['employee_attribute_inconsistencies']}")

    print("\n" + "=" * 80)
    print("PHASE 2: Building paie_fact_ready dataset")
    print("=" * 80)
    fact_ready_count, fact_stats = build_paie_fact_ready(dim_employee_count)
    print(f"✓ paie_fact_ready built: {fact_ready_count} rows")

    print("\n" + "=" * 80)
    print("PHASE 3: Validating consistency")
    print("=" * 80)
    paie_total_rows = inconsistency_report["paie_total_rows"]
    validation = validate_consistency(paie_total_rows, dim_employee_count, fact_ready_count)
    for check, result in validation["detail"].items():
        print(f"  {check}: {result}")

    print("\n" + "=" * 80)
    print("PHASE 4: Generating build report")
    print("=" * 80)

    sample_dim_employee = []
    sample_paie_fact = []

    with DIM_EMPLOYEE_OUTPUT.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i < 3:
                sample_dim_employee.append(json.loads(line))
            else:
                break

    with PAIE_FACT_READY_OUTPUT.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i < 3:
                sample_paie_fact.append(json.loads(line))
            else:
                break

    report = {
        "version_tag": "dim_paie_v1",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "phase_1_dim_employee": {
            "output_file": str(DIM_EMPLOYEE_OUTPUT),
            "total_employees": dim_employee_count,
            "inconsistencies": inconsistency_report["inconsistency_summary"],
            "sample": sample_dim_employee,
        },
        "phase_2_paie_fact_ready": {
            "output_file": str(PAIE_FACT_READY_OUTPUT),
            "total_rows": fact_ready_count,
            "unmapped_employees": fact_stats["unmapped_rows"],
            "sample": sample_paie_fact,
        },
        "phase_3_validation": validation,
        "summary": {
            "status": "PASS" if validation["paie_rows_preserved"] else "FAIL",
            "guarantees": [
                "All paie rows preserved (no deletions)",
                "No values fabricated",
                "No blind null/zero replacements",
                "Employee attributes resolved by frequency then recency",
                "Full traceability via resolution_method fields",
            ],
        },
    }

    BUILD_REPORT_OUTPUT.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"✓ Build report saved to {BUILD_REPORT_OUTPUT}")

    print("\n" + "=" * 80)
    print("BUILD COMPLETE")
    print("=" * 80)
    print(f"\nGenerated files:")
    print(f"  • {DIM_EMPLOYEE_OUTPUT.relative_to(PROJECT_ROOT)}")
    print(f"  • {PAIE_FACT_READY_OUTPUT.relative_to(PROJECT_ROOT)}")
    print(f"  • {BUILD_REPORT_OUTPUT.relative_to(PROJECT_ROOT)}")
    print(f"\nReady for DW load:")
    print(f"  ✓ dim_employee ready for loading into dim_employee table")
    print(f"  ✓ paie_fact_ready ready for loading into fact_paie (join with dim_employee on employee_key)")
    print(f"\nNext steps:")
    print(f"  1. Build dim_temps from paie (pa_annee + pa_mois)")
    print(f"  2. Load dim_employee and dim_temps into DW")
    print(f"  3. Build fact_paie by joining paie_fact_ready with dimensions")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Build dim_employee and paie_fact_ready datasets from cleaned paie."
    )
    parser.parse_args()
    run_build()


if __name__ == "__main__":
    main()
