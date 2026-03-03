import argparse
import csv
import json
import os
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from clean_raw_to_jsonl import clean_raw_to_jsonl
from recover_ind2015 import recover_ind2015


ROOT_DIR = Path(__file__).resolve().parents[1]
CLEAN_DIR = ROOT_DIR / "data" / "clean"
RAW_DIR = ROOT_DIR / "data" / "raw"
SYSTEM_COLUMNS = {"load_ts", "source_name"}
WRAPPER_KEYS = ["data", "payload", "rows", "records", "result", "results", "items"]

SOURCES = [
    {
        "name": "paie2015",
        "file": RAW_DIR / "paie2015.json",
        "clean_file": CLEAN_DIR / "paie2015.jsonl",
        "stage_table": "staging.stg_paie2015",
    },
    {
        "name": "ind2015",
        "file": RAW_DIR / "ind2015.json",
        "preferred_clean_files": [
            CLEAN_DIR / "ind2015_recovered.jsonl",
            CLEAN_DIR / "ind2015.jsonl",
        ],
        "clean_file": CLEAN_DIR / "ind2015.jsonl",
        "stage_table": "staging.stg_ind2015",
    },
    {
        "name": "grade",
        "file": RAW_DIR / "grade.json",
        "stage_table": "staging.stg_grade",
    },
    {
        "name": "nature",
        "file": RAW_DIR / "nature.json",
        "stage_table": "staging.stg_nature",
    },
    {
        "name": "region",
        "file": RAW_DIR / "region.json",
        "stage_table": "staging.stg_region",
    },
    {
        "name": "organisme",
        "file": RAW_DIR / "organisme.json",
        "stage_table": "staging.stg_organisme",
    },
    {
        "name": "indem_def",
        "file": RAW_DIR / "indem_def.json",
        "stage_table": "staging.stg_indem_def",
    },
]


def get_conn():
    return psycopg2.connect(
        host=os.getenv("INSAF_PGHOST", "localhost"),
        port=int(os.getenv("INSAF_PGPORT", "5432")),
        dbname=os.getenv("INSAF_PGDATABASE", "insaf_dw"),
        user=os.getenv("INSAF_PGUSER", "insaf"),
        password=os.getenv("INSAF_PGPASSWORD", "insaf"),
    )


def normalize_name(name: Any) -> str:
    return str(name).strip().lower()


def normalize_value(value: Any):
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped != "" else None
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalize_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    return {normalize_name(k): normalize_value(v) for k, v in row.items()}


def get_stage_columns(conn, stage_table: str) -> List[str]:
    schema_name, table_name = stage_table.split(".", 1)
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema_name, table_name))
        rows = cur.fetchall()
    return [row[0] for row in rows]


def truncate_stage_table(conn, stage_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {stage_table}")
    conn.commit()


def table_count(conn, stage_table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {stage_table}")
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _extract_rows_any(obj: Any, inherited_columns: Optional[List[str]] = None, depth: int = 0) -> List[Dict[str, Any]]:
    if depth > 30:
        return []

    if isinstance(obj, list):
        if not obj:
            return []
        if all(isinstance(item, dict) for item in obj):
            if all("items" in item or "results" in item or "result" in item for item in obj):
                rows: List[Dict[str, Any]] = []
                for item in obj:
                    rows.extend(_extract_rows_any(item, inherited_columns, depth + 1))
                return rows
            return [normalize_row_keys(item) for item in obj]

        if all(isinstance(item, list) for item in obj) and inherited_columns:
            rows: List[Dict[str, Any]] = []
            for raw in obj:
                rows.append({
                    inherited_columns[idx]: normalize_value(raw[idx])
                    for idx in range(min(len(inherited_columns), len(raw)))
                })
            return rows

        rows: List[Dict[str, Any]] = []
        for item in obj:
            if isinstance(item, (dict, list)):
                rows.extend(_extract_rows_any(item, inherited_columns, depth + 1))
        return rows

    if isinstance(obj, dict):
        columns: Optional[List[str]] = None
        if isinstance(obj.get("columns"), list):
            columns = []
            for col in obj["columns"]:
                if isinstance(col, dict) and "name" in col:
                    columns.append(normalize_name(col["name"]))
                elif isinstance(col, str):
                    columns.append(normalize_name(col))
        if not columns:
            columns = inherited_columns

        if "items" in obj:
            rows = _extract_rows_any(obj["items"], columns, depth + 1)
            if rows:
                return rows

        for key in WRAPPER_KEYS:
            if key in obj:
                rows = _extract_rows_any(obj[key], columns, depth + 1)
                if rows:
                    return rows

        for value in obj.values():
            if isinstance(value, (dict, list)):
                rows = _extract_rows_any(value, columns, depth + 1)
                if rows:
                    return rows

        return [normalize_row_keys(obj)]

    return []


def parse_json_rows(file_path: Path) -> List[Dict[str, Any]]:
    with file_path.open("r", encoding="utf-8", errors="ignore") as fh:
        payload = json.load(fh, strict=False)
    return _extract_rows_any(payload)


def clean_large_files(debug: bool = False) -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    for source in SOURCES:
        if "clean_file" not in source:
            continue
        result = clean_raw_to_jsonl(
            input_path=source["file"],
            output_path=source["clean_file"],
            progress_mb=50,
            required_key="pa_mat",
            debug=debug,
        )
        print(
            f"CLEAN [{source['name']}] output={result['output_path']} "
            f"written={result['written']} rejected={result['rejected']} bytes={result['bytes_read']}"
        )

        if source["name"] == "ind2015":
            recovered_path = CLEAN_DIR / "ind2015_recovered.jsonl"
            rec = recover_ind2015(
                input_path=source["file"],
                output_path=recovered_path,
                progress_mb=50,
                diagnose=False,
            )
            print(
                f"RECOVER [ind2015] output={rec['output_path']} "
                f"recovered_rows={rec['total_recovered_rows']}"
            )


def choose_source_path(source: Dict[str, Any]) -> Path:
    preferred = source.get("preferred_clean_files")
    if preferred:
        for candidate in preferred:
            path = Path(candidate)
            if path.exists():
                return path

    clean_path = source.get("clean_file")
    if clean_path and Path(clean_path).exists():
        return Path(clean_path)
    return source["file"]


def build_matched_columns(sample_keys: List[str], stage_columns: List[str], source_name: str) -> List[str]:
    data_stage_cols = [col for col in stage_columns if col not in SYSTEM_COLUMNS]
    key_set = set(sample_keys)
    matched = [col for col in data_stage_cols if col in key_set]

    if not matched:
        print(f"DEBUG [{source_name}] 0 matched columns")
        print(f"DEBUG [{source_name}] staging columns: {data_stage_cols}")
        print(f"DEBUG [{source_name}] sample source keys: {sample_keys[:120]}")
    return matched


def jsonl_to_csv_for_copy(
    jsonl_path: Path,
    stage_columns: List[str],
    source_name: str,
    require_pa_mat: bool,
) -> Tuple[Path, List[str], int, int]:
    matched_cols: Optional[List[str]] = None
    rows_written = 0
    rejected = 0

    temp = tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False, suffix=".csv")
    temp_path = Path(temp.name)

    with jsonl_path.open("r", encoding="utf-8", errors="ignore") as src, temp:
        writer: Optional[csv.DictWriter] = None

        for line in src:
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text, strict=False)
            except json.JSONDecodeError:
                rejected += 1
                continue
            if not isinstance(obj, dict):
                rejected += 1
                continue

            row = normalize_row_keys(obj)
            if require_pa_mat and not row.get("pa_mat"):
                rejected += 1
                continue

            if matched_cols is None:
                matched_cols = build_matched_columns(list(row.keys()), stage_columns, source_name)
                if not matched_cols:
                    break
                insert_cols = matched_cols + ["source_name"]
                writer = csv.DictWriter(temp, fieldnames=insert_cols)
                writer.writeheader()

            assert writer is not None
            out_row = {col: row.get(col) for col in matched_cols}
            out_row["source_name"] = source_name
            writer.writerow(out_row)
            rows_written += 1

    if not matched_cols:
        return temp_path, [], 0, rejected

    return temp_path, matched_cols + ["source_name"], rows_written, rejected


def copy_csv_to_table(conn, stage_table: str, csv_path: Path, insert_columns: List[str]) -> None:
    cols_sql = ", ".join(insert_columns)
    sql = f"COPY {stage_table} ({cols_sql}) FROM STDIN WITH CSV HEADER"
    with csv_path.open("r", encoding="utf-8") as fh:
        with conn.cursor() as cur:
            cur.copy_expert(sql, fh)
    conn.commit()


def load_jsonl_with_insert(
    conn,
    jsonl_path: Path,
    stage_table: str,
    stage_columns: List[str],
    source_name: str,
    require_pa_mat: bool,
) -> Tuple[int, int]:
    data_stage_cols = [col for col in stage_columns if col not in SYSTEM_COLUMNS]
    matched_cols: Optional[List[str]] = None
    rejected = 0
    inserted = 0
    batch: List[Tuple[Any, ...]] = []

    with conn.cursor() as cur:
        with jsonl_path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text, strict=False)
                except json.JSONDecodeError:
                    rejected += 1
                    continue
                if not isinstance(obj, dict):
                    rejected += 1
                    continue

                row = normalize_row_keys(obj)
                if require_pa_mat and not row.get("pa_mat"):
                    rejected += 1
                    continue

                if matched_cols is None:
                    matched_cols = [col for col in data_stage_cols if col in set(row.keys())]
                    if not matched_cols:
                        return 0, rejected

                insert_cols = matched_cols + ["source_name"]
                values = [row.get(col) for col in matched_cols] + [source_name]
                batch.append(tuple(values))

                if len(batch) >= 10000:
                    insert_sql = f"INSERT INTO {stage_table} ({', '.join(insert_cols)}) VALUES %s"
                    execute_values(cur, insert_sql, batch, page_size=2000)
                    inserted += len(batch)
                    batch.clear()

        if batch and matched_cols:
            insert_cols = matched_cols + ["source_name"]
            insert_sql = f"INSERT INTO {stage_table} ({', '.join(insert_cols)}) VALUES %s"
            execute_values(cur, insert_sql, batch, page_size=2000)
            inserted += len(batch)

    conn.commit()
    return inserted, rejected


def load_from_jsonl(
    conn,
    source: Dict[str, Any],
    stage_columns: List[str],
    use_copy: bool,
) -> int:
    source_name = source["name"]
    source_path = choose_source_path(source)
    stage_table = source["stage_table"]
    require_pa_mat = source_name in {"paie2015", "ind2015"}

    if use_copy:
        csv_path, insert_columns, rows_written, rejected = jsonl_to_csv_for_copy(
            jsonl_path=source_path,
            stage_columns=stage_columns,
            source_name=source_name,
            require_pa_mat=require_pa_mat,
        )
        try:
            if not insert_columns:
                print(f"TODO [{source_name}] no matched columns for COPY")
                return 0
            if rows_written == 0:
                print(f"TODO [{source_name}] no rows to COPY (rejected={rejected})")
                return 0
            copy_csv_to_table(conn, stage_table, csv_path, insert_columns)
            print(
                f"Loaded [{source_name}] from {source_path.name} via COPY "
                f"rows_inserted={rows_written} rejected={rejected}"
            )
            return rows_written
        finally:
            try:
                csv_path.unlink(missing_ok=True)
            except Exception:
                pass

    inserted, rejected = load_jsonl_with_insert(
        conn=conn,
        jsonl_path=source_path,
        stage_table=stage_table,
        stage_columns=stage_columns,
        source_name=source_name,
        require_pa_mat=require_pa_mat,
    )
    print(
        f"Loaded [{source_name}] from {source_path.name} via execute_values "
        f"rows_inserted={inserted} rejected={rejected}"
    )
    return inserted


def load_from_json(conn, source: Dict[str, Any], stage_columns: List[str]) -> int:
    source_name = source["name"]
    source_path = source["file"]
    stage_table = source["stage_table"]

    rows = parse_json_rows(source_path)
    if not rows:
        print(f"TODO [{source_name}] no rows parsed from {source_path.name}")
        return 0

    sample_keys = sorted(list(rows[0].keys())) if rows else []
    matched_cols = build_matched_columns(sample_keys, stage_columns, source_name)
    if not matched_cols:
        print(f"TODO [{source_name}] no insert due to 0 matched columns")
        return 0

    insert_cols = matched_cols + ["source_name"]
    insert_sql = f"INSERT INTO {stage_table} ({', '.join(insert_cols)}) VALUES %s"

    inserted = 0
    batch: List[Tuple[Any, ...]] = []
    with conn.cursor() as cur:
        for row in rows:
            normalized = normalize_row_keys(row)
            values = [normalized.get(col) for col in matched_cols] + [source_name]
            batch.append(tuple(values))
            if len(batch) >= 10000:
                execute_values(cur, insert_sql, batch, page_size=2000)
                inserted += len(batch)
                batch.clear()
        if batch:
            execute_values(cur, insert_sql, batch, page_size=2000)
            inserted += len(batch)
    conn.commit()

    print(
        f"Loaded [{source_name}] from {source_path.name} via JSON parser "
        f"rows_inserted={inserted}"
    )
    return inserted


def load_source(conn, source: Dict[str, Any], truncate: bool = False, use_copy: bool = False) -> int:
    source_name = source["name"]
    stage_table = source["stage_table"]

    stage_columns = get_stage_columns(conn, stage_table)
    if not stage_columns:
        raise RuntimeError(f"Stage table not found: {stage_table}")

    if truncate:
        truncate_stage_table(conn, stage_table)
        print(f"Truncated {stage_table}")

    source_path = choose_source_path(source)
    print(f"Loading [{source_name}] from {source_path.name} into {stage_table}")

    preferred = source.get("preferred_clean_files")
    if preferred:
        preferred_names = [Path(p).name for p in preferred]
        print(f"Source priority [{source_name}]: {preferred_names}")

    if source_path.suffix.lower() == ".jsonl":
        return load_from_jsonl(conn, source, stage_columns, use_copy=use_copy)

    return load_from_json(conn, source, stage_columns)


def run_self_test(conn) -> bool:
    failed = False
    print("Running self-test...")
    for source in SOURCES:
        stage_table = source["stage_table"]
        source_path = choose_source_path(source)
        rows = table_count(conn, stage_table)
        file_size = source_path.stat().st_size if source_path.exists() else 0

        status = "OK"
        if rows == 0 and file_size > 0:
            status = "FAIL"
            failed = True
        elif rows == 0 and file_size == 0:
            status = "EMPTY_SOURCE"

        print(
            f"SELFTEST [{source['name']}] table={stage_table} rows={rows} "
            f"source={source_path.name} source_size_bytes={file_size} status={status}"
        )

    if failed:
        print("SELFTEST RESULT: FAILED")
        return False
    print("SELFTEST RESULT: PASSED")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load JSON sources into Postgres staging")
    parser.add_argument("--truncate", action="store_true", help="TRUNCATE staging tables before loading")
    parser.add_argument("--clean-first", action="store_true", help="Create clean JSONL for paie2015 and ind2015 first")
    parser.add_argument("--use-copy", action="store_true", help="Load JSONL sources through CSV + COPY")
    parser.add_argument("--self-test", action="store_true", help="Validate staging counts after load")
    parser.add_argument("--debug", action="store_true", help="Print extra debug output")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.clean_first:
        clean_large_files(debug=args.debug)

    conn = get_conn()
    total_inserted = 0
    try:
        for source in SOURCES:
            try:
                inserted = load_source(
                    conn=conn,
                    source=source,
                    truncate=args.truncate,
                    use_copy=args.use_copy,
                )
                total_inserted += inserted
            except Exception as exc:
                conn.rollback()
                print(f"TODO [{source['name']}] load failed: {exc}")

        print(f"Total inserted rows across staging tables: {total_inserted}")

        if args.self_test:
            ok = run_self_test(conn)
            if not ok:
                raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
