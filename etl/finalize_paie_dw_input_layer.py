from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORTS_DIR = PROJECT_ROOT / "reports"


EMPLOYEE_BASELINE = CLEAN_DIR / "employee_production.jsonl"
DIM_EMPLOYEE_PROD = CLEAN_DIR / "dim_employee_production.jsonl"
PAIE_FACT_READY_PROD = CLEAN_DIR / "paie_fact_ready_production.jsonl"

DIM_GRADE_PROD = CLEAN_DIR / "dim_grade_production.jsonl"
DIM_NATURE_PROD = CLEAN_DIR / "dim_nature_production.jsonl"
DIM_REGION_PROD = CLEAN_DIR / "dim_region_production.jsonl"
DIM_ORGANISME_PROD = CLEAN_DIR / "dim_organisme_production.jsonl"
DIM_TIME_PROD = CLEAN_DIR / "dim_time_production.jsonl"

FINAL_REPORT = REPORTS_DIR / "paie_dw_input_layer_finalization.json"


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


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


def extract_raw_items(path: Path) -> List[Dict[str, Any]]:
    payload = read_json(path)
    return payload["results"][0]["items"]


def non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


def find_null_only_columns(rows: List[Dict[str, Any]], columns: List[str]) -> List[str]:
    null_only: List[str] = []
    for c in columns:
        if all(not non_empty(row.get(c)) for row in rows):
            null_only.append(c)
    return sorted(null_only)


def build_dimension_from_raw(
    raw_file: Path,
    output_file: Path,
    key_name: str,
    business_key_fields: List[str],
) -> Dict[str, Any]:
    rows = extract_raw_items(raw_file)
    columns = sorted({k for r in rows for k in r.keys()})
    null_only_cols = find_null_only_columns(rows, columns)

    keep_cols = [c for c in columns if c not in null_only_cols]

    def business_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
        return tuple(row.get(k) for k in business_key_fields)

    sorted_rows = sorted(rows, key=business_key)
    out_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(sorted_rows, start=1):
        rec = {key_name: idx}
        for c in keep_cols:
            rec[c] = row.get(c)
        out_rows.append(rec)

    written = write_jsonl(output_file, out_rows)

    return {
        "source": str(raw_file.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "target": str(output_file.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "input_rows": len(rows),
        "output_rows": written,
        "rows_preserved": len(rows) == written,
        "removed_columns": null_only_cols,
        "kept_columns": [key_name] + keep_cols,
    }


def build_dim_time_from_fact() -> Dict[str, Any]:
    pairs = set()
    for row in iter_jsonl(PAIE_FACT_READY_PROD):
        pairs.add((row.get("pa_annee"), row.get("pa_mois")))

    sorted_pairs = sorted(pairs)
    out_rows: List[Dict[str, Any]] = []
    for idx, (year, month) in enumerate(sorted_pairs, start=1):
        out_rows.append(
            {
                "time_key": idx,
                "pa_annee": year,
                "pa_mois": month,
                "year_month": f"{year:04d}-{month:02d}",
                "month_start_date": f"{year:04d}-{month:02d}-01",
            }
        )

    written = write_jsonl(DIM_TIME_PROD, out_rows)
    return {
        "source": str(PAIE_FACT_READY_PROD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "target": str(DIM_TIME_PROD.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "grain": "month",
        "input_rows": sum(1 for _ in iter_jsonl(PAIE_FACT_READY_PROD)),
        "output_rows": written,
        "fields": ["time_key", "pa_annee", "pa_mois", "year_month", "month_start_date"],
    }


def ensure_employee_production() -> Dict[str, Any]:
    if EMPLOYEE_BASELINE.exists():
        return {
            "status": "already_exists",
            "path": str(EMPLOYEE_BASELINE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        }

    if not DIM_EMPLOYEE_PROD.exists():
        raise FileNotFoundError(
            "Missing both employee_production.jsonl and dim_employee_production.jsonl"
        )

    rows_written = write_jsonl(EMPLOYEE_BASELINE, iter_jsonl(DIM_EMPLOYEE_PROD))
    return {
        "status": "created_from_dim_employee_production",
        "path": str(EMPLOYEE_BASELINE.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "rows": rows_written,
    }


def delete_if_exists(path: Path, reason: str) -> Dict[str, Any]:
    if path.exists():
        path.unlink()
        return {
            "path": str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "deleted": True,
            "reason": reason,
        }
    return {
        "path": str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "deleted": False,
        "reason": "file_not_present",
    }


def main() -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    employee_status = ensure_employee_production()

    grade_result = build_dimension_from_raw(
        raw_file=RAW_DIR / "grade.json",
        output_file=DIM_GRADE_PROD,
        key_name="grade_key",
        business_key_fields=["codgrd"],
    )
    nature_result = build_dimension_from_raw(
        raw_file=RAW_DIR / "nature.json",
        output_file=DIM_NATURE_PROD,
        key_name="nature_key",
        business_key_fields=["codnat"],
    )
    region_result = build_dimension_from_raw(
        raw_file=RAW_DIR / "region.json",
        output_file=DIM_REGION_PROD,
        key_name="region_key",
        business_key_fields=["coddep", "codreg"],
    )
    organisme_result = build_dimension_from_raw(
        raw_file=RAW_DIR / "organisme.json",
        output_file=DIM_ORGANISME_PROD,
        key_name="organisme_key",
        business_key_fields=["codetab", "cab", "sg", "dg", "dire", "sdir", "serv", "unite"],
    )
    time_result = build_dim_time_from_fact()

    deletion_actions = [
        delete_if_exists(
            CLEAN_DIR / "dim_employee.jsonl",
            "Superseded intermediate; replaced by employee_production.jsonl baseline.",
        ),
        delete_if_exists(
            CLEAN_DIR / "dim_employee_production.jsonl",
            "Duplicate naming variant; canonical baseline is employee_production.jsonl.",
        ),
        delete_if_exists(
            CLEAN_DIR / "paie_fact_ready.jsonl",
            "Superseded intermediate; canonical fact input is paie_fact_ready_production.jsonl.",
        ),
        delete_if_exists(
            CLEAN_DIR / "payroll_type1_clean.jsonl",
            "Cleaning-stage intermediate; reproducible from raw via etl/build_payroll_dataset.py.",
        ),
    ]

    final_clean_files = sorted([p.name for p in CLEAN_DIR.glob("*.jsonl")])

    report = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "scope": "paie_dw_type1_only",
        "approved_baselines": {
            "employee": "data/clean/employee_production.jsonl",
            "paie_fact_ready": "data/clean/paie_fact_ready_production.jsonl",
        },
        "employee_baseline_status": employee_status,
        "generated_dimensions": {
            "dim_grade": grade_result,
            "dim_nature": nature_result,
            "dim_region": region_result,
            "dim_organisme": organisme_result,
            "dim_time": time_result,
        },
        "clean_folder_deletions": deletion_actions,
        "final_clean_folder": final_clean_files,
    }

    FINAL_REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("Finalization complete")
    print(f"Report: {FINAL_REPORT}")
    print("Final clean files:")
    for name in final_clean_files:
        print(f"  - {name}")


if __name__ == "__main__":
    main()
