from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
REPORTS_DIR = PROJECT_ROOT / "reports"


PAIE_FILE = RAW_DIR / "paie2015.json"
GRADE_FILE = RAW_DIR / "grade.json"
NATURE_FILE = RAW_DIR / "nature.json"
ORGANISME_FILE = RAW_DIR / "organisme.json"
REGION_FILE = RAW_DIR / "region.json"


PAYROLL_OUTPUT = CLEAN_DIR / "payroll_type1_clean.jsonl"
TRACE_OUTPUT = REPORTS_DIR / "payroll_type1_field_trace.jsonl"
SUMMARY_OUTPUT = REPORTS_DIR / "payroll_type1_summary.json"
SUMMARY_MD_OUTPUT = REPORTS_DIR / "payroll_type1_summary.md"


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

DATE_FIELDS = {
    "pa_datnais",
    "pa_datent",
    "pa_date_ech",
    "pa_datnatu",
}

CODE_FIELDS = {
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


def iter_item_objects(path: Path) -> Iterator[str]:
    """Stream object payloads from the `items` array without loading full file."""
    with path.open("r", encoding="utf-8", errors="replace") as f:
        in_items = False
        in_string = False
        escape = False
        depth = 0
        obj_buf: list[str] = []

        for line in f:
            segment = line
            if not in_items:
                idx = segment.find('"items"')
                if idx == -1:
                    continue
                bracket = segment.find("[", idx)
                if bracket == -1:
                    in_items = True
                    continue
                in_items = True
                segment = segment[bracket + 1 :]

            for ch in segment:
                if depth == 0:
                    if ch == "]":
                        return
                    if ch == "{":
                        depth = 1
                        obj_buf = ["{"]
                        in_string = False
                        escape = False
                    continue

                obj_buf.append(ch)

                if in_string:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == '"':
                        in_string = False
                    continue

                if ch == '"':
                    in_string = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        yield "".join(obj_buf)
                        obj_buf = []


def parse_object_tokens(obj_text: str) -> Dict[str, str]:
    """Parse a flat JSON-like object where numeric values can be malformed."""
    i = 0
    n = len(obj_text)
    out: Dict[str, str] = {}

    while i < n and obj_text[i] != "{":
        i += 1
    i += 1

    while i < n:
        while i < n and obj_text[i] in " \t\r\n,":
            i += 1
        if i >= n or obj_text[i] == "}":
            break

        if obj_text[i] != '"':
            i += 1
            continue

        i += 1
        k_start = i
        while i < n and obj_text[i] != '"':
            i += 1
        key = obj_text[k_start:i]
        i += 1

        while i < n and obj_text[i] in " \t\r\n":
            i += 1
        if i < n and obj_text[i] == ":":
            i += 1

        while i < n and obj_text[i] in " \t\r\n":
            i += 1

        if i < n and obj_text[i] == '"':
            i += 1
            v_chars: list[str] = []
            escaped = False
            while i < n:
                ch = obj_text[i]
                if escaped:
                    v_chars.append(ch)
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    break
                else:
                    v_chars.append(ch)
                i += 1
            value = "".join(v_chars)
            i += 1
        else:
            v_chars: list[str] = []
            saw_decimal_comma = False
            while i < n:
                ch = obj_text[i]
                if ch == "}":
                    break
                if ch == ",":
                    j = i + 1
                    while j < n and obj_text[j] in " \t\r\n":
                        j += 1
                    # Decimal commas are followed by a digit, while field separators
                    # are followed by the next quoted key.
                    if (
                        not saw_decimal_comma
                        and v_chars
                        and v_chars[-1].isdigit()
                        and j < n
                        and obj_text[j].isdigit()
                    ):
                        v_chars.append(ch)
                        saw_decimal_comma = True
                        i += 1
                        continue
                    break
                v_chars.append(ch)
                i += 1
            value = "".join(v_chars).strip()

        out[key.lower()] = value

    return out


def parse_numeric(raw: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    value = raw.strip()
    if value == "":
        return None, "blank_numeric", None

    if re.fullmatch(r"-?\d+", value):
        return float(int(value)), None, None

    if re.fullmatch(r"-?\d+[.,]\d+", value):
        repaired = value.replace(",", ".")
        return float(repaired), "decimal_comma_to_dot" if "," in value else None, repaired

    if re.fullmatch(r"-?\d{1,3}(?:[ .]\d{3})+(?:[.,]\d+)?", value):
        repaired = value.replace(" ", "").replace(".", "")
        if "," in repaired:
            repaired = repaired.replace(",", ".")
        return float(repaired), "thousands_separator_removed", repaired

    return None, "unresolved_numeric", None


def parse_date(raw: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    value = raw.strip()
    if value == "":
        return None, "blank_date", None

    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{2})", value)
    if not m:
        return None, "unresolved_date", None

    day, month, yy = map(int, m.groups())
    year = 2000 + yy if yy <= 29 else 1900 + yy
    try:
        dt = datetime(year=year, month=month, day=day)
    except ValueError:
        return None, "invalid_date", None
    return dt.strftime("%Y-%m-%d"), "date_ddmmyy_to_iso", dt.strftime("%Y-%m-%d")


def clean_string(raw: str, is_code: bool) -> Tuple[str, Optional[str], Optional[str]]:
    if is_code:
        stripped = raw.strip()
        if stripped != raw:
            return stripped, "trim_code_whitespace", stripped
        return raw, None, None

    normalized = re.sub(r"\s+", " ", raw).strip()
    if normalized != raw:
        return normalized, "normalize_text_whitespace", normalized
    return raw, None, None


def build_reference_maps() -> Dict[str, Dict[str, str]]:
    grade_map: Dict[str, str] = {}
    for obj in iter_item_objects(GRADE_FILE):
        tok = parse_object_tokens(obj)
        code = tok.get("codgrd", "").strip()
        label = tok.get("libcgrdl", "").strip() or tok.get("liblgrdl", "").strip()
        if code and label and code not in grade_map:
            grade_map[code] = label

    nature_map: Dict[str, str] = {}
    for obj in iter_item_objects(NATURE_FILE):
        tok = parse_object_tokens(obj)
        code = tok.get("codnat", "").strip()
        label = tok.get("libnatl", "").strip()
        if code and label and code not in nature_map:
            nature_map[code] = label

    org_map: Dict[str, str] = {}
    for obj in iter_item_objects(ORGANISME_FILE):
        tok = parse_object_tokens(obj)
        key = "|".join(
            [
                tok.get("codetab", "").strip(),
                tok.get("cab", "").strip(),
                tok.get("sg", "").strip(),
                tok.get("dg", "").strip(),
                tok.get("dire", "").strip(),
                tok.get("sdir", "").strip(),
                tok.get("serv", "").strip(),
                tok.get("unite", "").strip(),
            ]
        )
        label = tok.get("liborgl", "").strip()
        if key and label and key not in org_map:
            org_map[key] = label

    region_map: Dict[str, str] = {}
    for obj in iter_item_objects(REGION_FILE):
        tok = parse_object_tokens(obj)
        key = f"{tok.get('coddep', '').strip()}|{tok.get('codreg', '').strip()}"
        label = tok.get("lib_reg", "").strip()
        if key and label and key not in region_map:
            region_map[key] = label

    return {
        "grade": grade_map,
        "nature": nature_map,
        "organisme": org_map,
        "region": region_map,
    }


def transform_row(
    row_index: int,
    raw_tokens: Dict[str, str],
    refs: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, object], Dict[str, object], bool]:
    record: Dict[str, object] = {}
    field_trace: list[Dict[str, object]] = []
    unresolved_fields: list[str] = []

    for field, raw_value in raw_tokens.items():
        if field in NUMERIC_FIELDS:
            parsed, action, repaired = parse_numeric(raw_value)
            if parsed is None and action in {"unresolved_numeric", "blank_numeric"}:
                unresolved_fields.append(field)
                record[field] = None
            else:
                record[field] = int(parsed) if parsed is not None and parsed.is_integer() else parsed
            if action:
                field_trace.append(
                    {
                        "field": field,
                        "raw": raw_value,
                        "clean": record.get(field),
                        "action": action,
                        "repaired_token": repaired,
                    }
                )
            continue

        if field in DATE_FIELDS:
            parsed, action, repaired = parse_date(raw_value)
            if parsed is None and action in {"unresolved_date", "invalid_date", "blank_date"}:
                unresolved_fields.append(field)
                record[field] = None
            else:
                record[field] = parsed
            if action:
                field_trace.append(
                    {
                        "field": field,
                        "raw": raw_value,
                        "clean": record.get(field),
                        "action": action,
                        "repaired_token": repaired,
                    }
                )
            continue

        cleaned, action, repaired = clean_string(raw_value, field in CODE_FIELDS)
        record[field] = cleaned
        if action:
            field_trace.append(
                {
                    "field": field,
                    "raw": raw_value,
                    "clean": cleaned,
                    "action": action,
                    "repaired_token": repaired,
                }
            )

    record.setdefault("pa_type", "")
    is_type1 = str(record.get("pa_type", "")).strip() == "1"

    org_key = "|".join(
        [
            str(record.get("pa_codmin", "")),
            str(record.get("pa_cab", "")),
            str(record.get("pa_sg", "")),
            str(record.get("pa_dg", "")),
            str(record.get("pa_dire", "")),
            str(record.get("pa_sdir", "")),
            str(record.get("pa_serv", "")),
            str(record.get("pa_unite", "")),
        ]
    )
    record["ref_grade_label"] = refs["grade"].get(str(record.get("pa_grd", "")), None)
    record["ref_nature_label"] = refs["nature"].get(str(record.get("pa_natu", "")), None)
    record["ref_organisme_label"] = refs["organisme"].get(org_key, None)
    reg_key = f"{record.get('pa_codmin', '')}|{record.get('pa_loca', '')}"
    record["ref_region_label"] = refs["region"].get(reg_key, None)

    record["dq_unresolved_fields"] = sorted(set(unresolved_fields))
    record["dq_has_unresolved"] = len(record["dq_unresolved_fields"]) > 0
    record["dq_trace_count"] = len(field_trace)
    record["_row_index"] = row_index

    trace_payload = {
        "row_index": row_index,
        "pa_mat": record.get("pa_mat"),
        "pa_annee": record.get("pa_annee"),
        "pa_mois": record.get("pa_mois"),
        "pa_type": record.get("pa_type"),
        "unresolved_fields": record["dq_unresolved_fields"],
        "field_trace": field_trace,
    }

    return record, trace_payload, is_type1


def run_pipeline(max_rows: Optional[int] = None) -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    refs = build_reference_maps()

    stats = Counter()
    unresolved_by_field = Counter()

    with PAYROLL_OUTPUT.open("w", encoding="utf-8") as clean_out, TRACE_OUTPUT.open(
        "w", encoding="utf-8"
    ) as trace_out:
        for idx, obj_text in enumerate(iter_item_objects(PAIE_FILE), start=1):
            if max_rows is not None and idx > max_rows:
                break

            stats["rows_extracted"] += 1
            raw_tokens = parse_object_tokens(obj_text)

            record, trace_payload, is_type1 = transform_row(idx, raw_tokens, refs)

            if not is_type1:
                stats["rows_non_type1"] += 1
                continue

            stats["rows_type1"] += 1
            if record["dq_has_unresolved"]:
                stats["rows_with_unresolved"] += 1
                for f in record["dq_unresolved_fields"]:
                    unresolved_by_field[f] += 1

            if trace_payload["field_trace"]:
                stats["rows_with_repairs"] += 1

            clean_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            trace_out.write(json.dumps(trace_payload, ensure_ascii=False) + "\n")

    summary = {
        "source": str(PAIE_FILE),
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "counts": {
            "rows_extracted": stats["rows_extracted"],
            "rows_type1": stats["rows_type1"],
            "rows_non_type1": stats["rows_non_type1"],
            "rows_with_repairs": stats["rows_with_repairs"],
            "rows_with_unresolved": stats["rows_with_unresolved"],
        },
        "unresolved_by_field": dict(unresolved_by_field),
        "references": {
            "grade_rows": len(refs["grade"]),
            "nature_rows": len(refs["nature"]),
            "organisme_rows": len(refs["organisme"]),
            "region_rows": len(refs["region"]),
        },
        "outputs": {
            "clean_jsonl": str(PAYROLL_OUTPUT),
            "trace_jsonl": str(TRACE_OUTPUT),
        },
    }

    SUMMARY_OUTPUT.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    md_lines = [
        "# Payroll Type-1 ETL Summary",
        "",
        "## Scope",
        "- Source: data/raw/paie2015.json",
        "- Included rows: pa_type == 1 only",
        "- Rows are never dropped for data quality reasons; unresolved fields are explicitly flagged.",
        "",
        "## Row Counts",
        f"- Extracted raw objects: {summary['counts']['rows_extracted']}",
        f"- Payroll rows (type 1): {summary['counts']['rows_type1']}",
        f"- Non-payroll rows skipped from this deliverable: {summary['counts']['rows_non_type1']}",
        f"- Payroll rows with at least one repair: {summary['counts']['rows_with_repairs']}",
        f"- Payroll rows with unresolved fields: {summary['counts']['rows_with_unresolved']}",
        "",
        "## Output Files",
        "- data/clean/payroll_type1_clean.jsonl",
        "- reports/payroll_type1_field_trace.jsonl",
        "- reports/payroll_type1_summary.json",
        "",
        "## Unresolved Fields",
    ]

    if unresolved_by_field:
        for field, count in unresolved_by_field.most_common():
            md_lines.append(f"- {field}: {count}")
    else:
        md_lines.append("- None")

    SUMMARY_MD_OUTPUT.write_text("\n".join(md_lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build cleaned payroll type-1 dataset from malformed raw source.")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional limit for development profiling.")
    args = parser.parse_args()
    run_pipeline(max_rows=args.max_rows)


if __name__ == "__main__":
    main()
