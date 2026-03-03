import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterator, Optional


def iter_object_fragments(
    file_path: Path,
    progress_mb: int = 50,
    max_object_bytes: int = 5_000_000,
) -> Iterator[str]:
    in_string = False
    escaped = False
    depth = 0
    collecting = False
    current: list[str] = []

    bytes_read = 0
    next_report = progress_mb * 1024 * 1024

    with file_path.open("rb") as fh:
        while True:
            chunk = fh.read(65536)
            if not chunk:
                break

            bytes_read += len(chunk)
            if bytes_read >= next_report:
                print(
                    f"Progress [{file_path.name}] bytes_read={bytes_read}",
                    flush=True,
                )
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

                if len(current) >= max_object_bytes:
                    collecting = False
                    current = []
                    depth = 0
                    in_string = False
                    escaped = False


def clean_raw_to_jsonl(
    input_path: Path,
    output_path: Path,
    progress_mb: int = 50,
    required_key: str = "pa_mat",
    debug: bool = False,
) -> Dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    rejected = 0
    key_counter: Counter[str] = Counter()

    with output_path.open("w", encoding="utf-8", newline="\n") as out:
        for frag in iter_object_fragments(input_path, progress_mb=progress_mb):
            try:
                obj = json.loads(frag, strict=False)
            except json.JSONDecodeError:
                rejected += 1
                continue

            if not isinstance(obj, dict):
                rejected += 1
                continue

            normalized = {str(k).strip().lower(): v for k, v in obj.items()}
            if required_key and not normalized.get(required_key):
                rejected += 1
                continue

            out.write(json.dumps(normalized, ensure_ascii=False) + "\n")
            written += 1
            key_counter.update(normalized.keys())

    bytes_read = input_path.stat().st_size
    top_keys = key_counter.most_common(20)

    print(f"Clean complete: {input_path.name}")
    print(f"  output: {output_path}")
    print(f"  bytes_read: {bytes_read}")
    print(f"  objects_written: {written}")
    print(f"  objects_rejected: {rejected}")
    print(f"  top_keys: {top_keys}")

    return {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "bytes_read": bytes_read,
        "written": written,
        "rejected": rejected,
        "top_keys": top_keys,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover valid JSON objects from malformed raw file into JSONL")
    parser.add_argument("input", type=Path, help="Input raw JSON path")
    parser.add_argument("output", type=Path, help="Output JSONL path")
    parser.add_argument("--progress-mb", type=int, default=50, help="Progress print interval in MB")
    parser.add_argument("--required-key", type=str, default="pa_mat", help="Required key to keep a record")
    parser.add_argument("--debug", action="store_true", help="Reserved debug flag")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    clean_raw_to_jsonl(
        input_path=args.input,
        output_path=args.output,
        progress_mb=args.progress_mb,
        required_key=args.required_key,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
