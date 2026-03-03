import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

PAIE2015_BASELINE = 22867


@dataclass
class StrategyStats:
    name: str
    recovered: int = 0
    rejected: int = 0
    sample_keys: Optional[List[str]] = None


def normalize_decimal_commas_outside_strings(text: str) -> str:
    out: List[str] = []
    in_string = False
    escaped = False
    n = len(text)

    for i, ch in enumerate(text):
        if in_string:
            out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            out.append(ch)
            continue

        if ch == ",":
            prev_is_digit = i > 0 and text[i - 1].isdigit()
            next_is_digit = i + 1 < n and text[i + 1].isdigit()
            if prev_is_digit and next_is_digit:
                out.append(".")
                continue

        out.append(ch)

    return "".join(out)


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).strip().lower(): v for k, v in row.items()}


def is_valid_recovered_row(row: Dict[str, Any], min_keys: int = 10) -> bool:
    if not isinstance(row, dict):
        return False
    r = normalize_row(row)
    required = ("pa_mat", "pa_annee", "pa_mois")
    if any(k not in r or r[k] in (None, "", " ") for k in required):
        return False
    if len(r.keys()) < min_keys:
        return False
    return True


def make_row_fingerprint(row: Dict[str, Any]) -> str:
    r = normalize_row(row)
    return "|".join(
        [
            str(r.get("pa_mat", "")),
            str(r.get("pa_annee", "")),
            str(r.get("pa_mois", "")),
            str(r.get("pa_type", "")),
            str(r.get("pa_sec", "")),
            str(r.get("pa_codmin", "")),
            str(r.get("pa_article", "")),
            str(r.get("pa_parag", "")),
        ]
    )


def add_if_new(
    row: Dict[str, Any],
    writer,
    seen: Set[str],
    key_counter: Counter,
    stats: StrategyStats,
) -> None:
    normalized = normalize_row(row)
    fingerprint = make_row_fingerprint(normalized)
    if fingerprint in seen:
        stats.rejected += 1
        return

    writer.write(json.dumps(normalized, ensure_ascii=False) + "\n")
    seen.add(fingerprint)
    stats.recovered += 1
    key_counter.update(normalized.keys())
    if stats.sample_keys is None:
        stats.sample_keys = sorted(list(normalized.keys()))


def iter_object_fragments(file_path: Path, progress_mb: int = 50) -> Iterator[str]:
    in_string = False
    escaped = False
    depth = 0
    collecting = False
    current: List[str] = []

    bytes_read = 0
    next_report = progress_mb * 1024 * 1024

    with file_path.open("rb") as fh:
        while True:
            chunk = fh.read(65536)
            if not chunk:
                break
            bytes_read += len(chunk)
            if bytes_read >= next_report:
                print(f"Progress [brace-scan] bytes_read={bytes_read}", flush=True)
                next_report += progress_mb * 1024 * 1024

            text = chunk.decode("utf-8", errors="ignore")
            for ch in text:
                if not collecting:
                    if ch == "{":
                        collecting = True
                        current = ["{"]
                        depth = 1
                        in_string = False
                        escaped = False
                    continue

                current.append(ch)

                if in_string:
                    if escaped:
                        escaped = False
                    elif ch == "\\":
                        escaped = True
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
                        yield "".join(current)
                        collecting = False
                        current = []
                        continue

                if len(current) > 8_000_000:
                    collecting = False
                    current = []
                    depth = 0
                    in_string = False
                    escaped = False


def extract_columns_and_items_start(file_path: Path) -> Tuple[List[str], int]:
    marker_cols = '"columns"'
    marker_items = '"items"'

    buffer = ""
    bytes_read = 0
    chunk_size = 65536
    columns: List[str] = []
    items_start = -1

    with file_path.open("r", encoding="utf-8", errors="ignore") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            bytes_read += len(chunk)
            buffer += chunk

            if not columns and marker_cols in buffer:
                idx = buffer.find(marker_cols)
                block = buffer[idx: idx + 2_000_000]
                m = re.search(r'"columns"\s*:\s*\[(.*?)\]\s*,\s*"items"', block, flags=re.S)
                if m:
                    names = re.findall(r'"name"\s*:\s*"([^"]+)"', m.group(1))
                    columns = [name.strip().lower() for name in names]

            if items_start < 0 and marker_items in buffer:
                local_idx = buffer.find(marker_items)
                if local_idx >= 0:
                    prefix = buffer[:local_idx]
                    items_start = bytes_read - len(buffer) + local_idx
                    break

            if len(buffer) > 3_000_000:
                buffer = buffer[-1_000_000:]

    return columns, items_start


def iter_items_entries(file_path: Path, items_start: int, progress_mb: int = 50) -> Iterator[str]:
    if items_start < 0:
        return

    marker_re = re.compile(r'"items"\s*:\s*\[', flags=re.IGNORECASE)
    chunk_size = 65536

    with file_path.open("r", encoding="utf-8", errors="ignore") as fh:
        fh.seek(max(items_start - 64, 0))

        pre = fh.read(4096)
        m = marker_re.search(pre)
        if not m:
            return

        remainder = pre[m.end():]

        in_string = False
        escaped = False
        depth_curly = 0
        depth_square = 0
        current: List[str] = []
        collecting = False

        bytes_scanned = fh.tell()
        next_report = progress_mb * 1024 * 1024
        data = remainder

        while True:
            if not data:
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                data = chunk
                bytes_scanned += len(chunk)
                if bytes_scanned >= next_report:
                    print(f"Progress [items-parser] bytes_scanned={bytes_scanned}", flush=True)
                    next_report += progress_mb * 1024 * 1024

            ch = data[0]
            data = data[1:]

            if not collecting:
                if ch == "]":
                    break
                if ch in "[{":
                    collecting = True
                    current = [ch]
                    in_string = False
                    escaped = False
                    depth_curly = 1 if ch == "{" else 0
                    depth_square = 1 if ch == "[" else 0
                continue

            current.append(ch)

            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
            elif ch == "{":
                depth_curly += 1
            elif ch == "}":
                depth_curly -= 1
            elif ch == "[":
                depth_square += 1
            elif ch == "]":
                depth_square -= 1

            if depth_curly == 0 and depth_square == 0:
                yield "".join(current)
                collecting = False
                current = []


def strategy_a_columns_items(
    file_path: Path,
    writer,
    seen: Set[str],
    key_counter: Counter,
    progress_mb: int,
) -> StrategyStats:
    stats = StrategyStats(name="STRATEGY A — columns/items parser")
    columns, items_start = extract_columns_and_items_start(file_path)

    if items_start < 0:
        print(f"{stats.name}: items array not found")
        return stats

    for entry in iter_items_entries(file_path, items_start, progress_mb=progress_mb):
        cleaned = normalize_decimal_commas_outside_strings(entry)
        try:
            obj = json.loads(cleaned, strict=False)
        except json.JSONDecodeError:
            stats.rejected += 1
            continue

        row: Optional[Dict[str, Any]] = None
        if isinstance(obj, dict):
            row = obj
        elif isinstance(obj, list) and columns:
            row = {columns[idx]: obj[idx] for idx in range(min(len(columns), len(obj)))}

        if row is None or not is_valid_recovered_row(row):
            stats.rejected += 1
            continue

        add_if_new(row, writer, seen, key_counter, stats)

    print(f"{stats.name}: recovered={stats.recovered} rejected={stats.rejected} sample_keys={stats.sample_keys}")
    return stats


def strategy_b_raw_decode(
    file_path: Path,
    writer,
    seen: Set[str],
    key_counter: Counter,
    progress_mb: int,
) -> StrategyStats:
    stats = StrategyStats(name="STRATEGY B — concatenated object decoder")
    decoder = json.JSONDecoder(strict=False)
    chunk_size = 1024 * 1024

    buffer = ""
    bytes_read = 0
    next_report = progress_mb * 1024 * 1024

    with file_path.open("r", encoding="utf-8", errors="ignore") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            bytes_read += len(chunk)
            if bytes_read >= next_report:
                print(f"Progress [raw-decode] bytes_read={bytes_read}", flush=True)
                next_report += progress_mb * 1024 * 1024

            buffer += chunk
            idx = 0
            clean_buffer = normalize_decimal_commas_outside_strings(buffer)

            while idx < len(clean_buffer):
                while idx < len(clean_buffer) and clean_buffer[idx] not in "[{":
                    idx += 1
                if idx >= len(clean_buffer):
                    break

                try:
                    obj, end = decoder.raw_decode(clean_buffer, idx)
                except json.JSONDecodeError:
                    idx += 1
                    continue

                idx = end
                if not isinstance(obj, dict):
                    stats.rejected += 1
                    continue
                if not is_valid_recovered_row(obj, min_keys=10):
                    stats.rejected += 1
                    continue
                add_if_new(obj, writer, seen, key_counter, stats)

            if idx > 0:
                buffer = buffer[idx:]

            if len(buffer) > 5_000_000:
                buffer = buffer[-1_000_000:]

    print(f"{stats.name}: recovered={stats.recovered} rejected={stats.rejected} sample_keys={stats.sample_keys}")
    return stats


def strategy_c_brace_recovery(
    file_path: Path,
    writer,
    seen: Set[str],
    key_counter: Counter,
    progress_mb: int,
) -> StrategyStats:
    stats = StrategyStats(name="STRATEGY C — regex/brace object recovery")

    for frag in iter_object_fragments(file_path, progress_mb=progress_mb):
        cleaned = normalize_decimal_commas_outside_strings(frag)
        try:
            obj = json.loads(cleaned, strict=False)
        except json.JSONDecodeError:
            stats.rejected += 1
            continue

        if not isinstance(obj, dict) or not is_valid_recovered_row(obj):
            stats.rejected += 1
            continue

        add_if_new(obj, writer, seen, key_counter, stats)

    print(f"{stats.name}: recovered={stats.recovered} rejected={stats.rejected} sample_keys={stats.sample_keys}")
    return stats


def coerce_value(raw: str) -> Any:
    token = raw.strip()
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        val = token[1:-1]
        val = val.replace('\\"', '"').replace('\\\\', '\\')
        return val
    low = token.lower()
    if low == "null":
        return None
    if low == "true":
        return True
    if low == "false":
        return False
    if re.fullmatch(r"-?\d+", token):
        try:
            return int(token)
        except Exception:
            return token
    if re.fullmatch(r"-?\d+[\.,]\d+", token):
        try:
            return float(token.replace(",", "."))
        except Exception:
            return token
    return token


def extract_pa_pairs(block: str) -> Dict[str, Any]:
    pairs = {}
    pattern = re.compile(
        r'"(pa_[a-z0-9_]+)"\s*:\s*("(?:\\.|[^"\\])*"|-?\d+(?:[\.,]\d+)?|null|true|false)',
        flags=re.IGNORECASE,
    )
    for key, value in pattern.findall(block):
        pairs[key.lower()] = coerce_value(value)
    return pairs


def strategy_d_loose_kv(
    file_path: Path,
    writer,
    seen: Set[str],
    key_counter: Counter,
    progress_mb: int,
) -> StrategyStats:
    stats = StrategyStats(name="STRATEGY D — loose key-value recovery")

    for frag in iter_object_fragments(file_path, progress_mb=progress_mb):
        if "pa_mat" not in frag or "pa_annee" not in frag or "pa_mois" not in frag:
            stats.rejected += 1
            continue

        row = extract_pa_pairs(frag)
        if not row:
            stats.rejected += 1
            continue
        if not is_valid_recovered_row(row, min_keys=10):
            stats.rejected += 1
            continue

        add_if_new(row, writer, seen, key_counter, stats)

    print(f"{stats.name}: recovered={stats.recovered} rejected={stats.rejected} sample_keys={stats.sample_keys}")
    return stats


def diagnose_structure(file_path: Path, chunk_size: int = 200_000) -> None:
    b = file_path.read_bytes()
    n = len(b)
    mid_start = max(0, n // 2 - chunk_size // 2)
    slices = [
        ("first", 0, min(chunk_size, n)),
        ("middle", mid_start, min(mid_start + chunk_size, n)),
        ("last", max(0, n - chunk_size), n),
    ]

    print("=== Step 1 — Structural Diagnosis ===")
    print(f"file={file_path}")
    print(f"file_size_bytes={n}")

    whole = b.decode("utf-8", errors="ignore")
    print(
        "whole_counts "
        f"pa_mat={whole.count('pa_mat')} "
        f"opens={whole.count('{')} "
        f"items={whole.count('items')} "
        f"columns={whole.count('columns')}"
    )

    for name, start, end in slices:
        t = b[start:end].decode("utf-8", errors="ignore")
        print(
            f"[{name}] range={start}:{end} len={len(t)} "
            f"pa_mat={t.count('pa_mat')} opens={t.count('{')} "
            f"items={t.count('items')} columns={t.count('columns')}"
        )

    pattern_results = '"results"' in whole
    pattern_columns_items = ('"columns"' in whole) and ('"items"' in whole)
    pattern_concat_hint = ('\n,{' in whole) or ('},{' in whole)

    print("detected_patterns:")
    print(f"- columns+items format: {pattern_columns_items}")
    print(f"- wrapper key 'results': {pattern_results}")
    print(f"- concatenated objects hint (}},{{ / \\n,{{): {pattern_concat_hint}")
    print("- likely JSONL: False (single wrapper + items detected)")
    print("- likely Oracle-style export array with malformed numeric commas: True")


def recover_ind2015(
    input_path: Path,
    output_path: Path,
    progress_mb: int = 50,
    diagnose: bool = True,
) -> Dict[str, Any]:
    if diagnose:
        diagnose_structure(input_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    seen: Set[str] = set()
    key_counter: Counter = Counter()
    strategy_results: List[StrategyStats] = []

    with output_path.open("w", encoding="utf-8", newline="\n") as writer:
        strategy_results.append(strategy_a_columns_items(input_path, writer, seen, key_counter, progress_mb))
        strategy_results.append(strategy_b_raw_decode(input_path, writer, seen, key_counter, progress_mb))
        strategy_results.append(strategy_c_brace_recovery(input_path, writer, seen, key_counter, progress_mb))
        strategy_results.append(strategy_d_loose_kv(input_path, writer, seen, key_counter, progress_mb))

    total_recovered = len(seen)
    pct_of_paie = (total_recovered / PAIE2015_BASELINE) * 100 if PAIE2015_BASELINE else 0.0

    print("=== Step 3 — Output ===")
    print(f"output_path={output_path}")
    print(f"total_recovered_rows={total_recovered}")
    print(f"recovery_vs_paie2015={pct_of_paie:.2f}% (baseline={PAIE2015_BASELINE})")
    print(f"top_20_keys={key_counter.most_common(20)}")

    print("=== Step 4 — Conclusion ===")
    if pct_of_paie < 5.0:
        print("File likely incomplete or corrupted export. Clean source required.")
    else:
        print("Recovery is above 5% of paie2015 baseline.")

    return {
        "output_path": str(output_path),
        "total_recovered_rows": total_recovered,
        "recovery_vs_paie2015_pct": pct_of_paie,
        "strategy_results": strategy_results,
        "top_keys": key_counter.most_common(20),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forensic multi-strategy recovery for ind2015 malformed JSON")
    parser.add_argument("--input", type=Path, default=Path("data/raw/ind2015.json"), help="Input file path")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/clean/ind2015_recovered.jsonl"),
        help="Output JSONL path",
    )
    parser.add_argument("--progress-mb", type=int, default=50, help="Progress interval in MB")
    parser.add_argument("--no-diagnose", action="store_true", help="Skip structural diagnosis")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    recover_ind2015(
        input_path=args.input,
        output_path=args.output,
        progress_mb=args.progress_mb,
        diagnose=not args.no_diagnose,
    )


if __name__ == "__main__":
    main()
