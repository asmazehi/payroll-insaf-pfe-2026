"""
Universal file reader for all INSAF source formats.

Supports:
  - JSON  (Oracle nested: {"columns":[...], "items":[...]}  )
  - JSON  (Oracle results wrapper: {"results":[{"columns":...,"items":...}]})
  - JSONL (one JSON object per line)
  - CSV   (.csv)
  - Excel (.xlsx / .xls)

Special handling:
  - ind2015.json contains INVALID JSON because French decimal commas are
    written directly into numeric fields (e.g. "pa_indice":0,04).
    We repair this with a targeted regex before parsing.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)


# ── Format detection ─────────────────────────────────────────────────────────

def detect_format(path: Path) -> str:
    """
    Returns one of:
      'json_oracle'   — {"columns":[...], "items":[...]}  or results wrapper
      'jsonl'         — newline-delimited JSON
      'csv'           — comma/semicolon separated values
      'excel'         — .xlsx / .xls
    """
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        return "excel"
    if suffix in (".csv",):
        return "csv"
    if suffix in (".jsonl", ".ndjson"):
        return "jsonl"
    if suffix == ".json":
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            head = f.read(512)
        head = head.lstrip()
        if '"columns"' in head or '"results"' in head:
            return "json_oracle"
        if head.startswith("["):
            return "json_array"
        return "json_object"
    return "unknown"


# ── Malformed-JSON repair ─────────────────────────────────────────────────────

_COMMA_DECIMAL_RE = re.compile(
    r'(?<=\d),(?=\d)'          # digit , digit  — French decimal separator
    r'(?=[^"]*(?:"[^"]*"[^"]*)*[,}\]])'  # not inside a string value
)

def _repair_comma_decimals(text: str) -> str:
    """
    Replace French decimal commas in JSON numeric fields with dots.

    e.g.  "pa_indice":0,04  →  "pa_indice":0.04
          "pa_cpe":12,6     →  "pa_cpe":12.6

    Strategy: scan for the pattern  :DIGITS,DIGITS  (outside string context)
    and replace the comma with a dot.
    """
    # Simpler and more reliable: replace `:number,number` patterns
    return re.sub(
        r'(:\s*-?\d+),(\d)',
        r'\1.\2',
        text
    )


# ── Oracle-format JSON reader ─────────────────────────────────────────────────

def _stream_json_oracle(path: Path) -> Iterator[dict]:
    """
    Handle two Oracle export flavours:
      A)  {"columns":[...], "items":[[v,v,...], ...]}    (array-of-arrays)
      B)  {"results":[{"columns":[...], "items":[{...}, ...]}]}
      C)  {"results":[{"columns":[...], "items":[[v,...], ...]}]}

    Also repairs French comma decimals before parsing.
    """
    log.debug("Reading Oracle JSON: %s", path.name)
    raw = path.read_bytes()

    # Try UTF-8 first; fall back to latin-1 (never raises)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")

    # Repair comma decimals BEFORE json.loads
    text = _repair_comma_decimals(text)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        log.error("JSON parse failed for %s: %s", path.name, exc)
        raise

    # Unwrap "results" envelope if present
    if isinstance(data, dict) and "results" in data:
        block = data["results"][0]
    else:
        block = data

    columns: list[str] = [
        col["name"] if isinstance(col, dict) else col
        for col in block["columns"]
    ]

    for item in block["items"]:
        if isinstance(item, dict):
            # Already a dict (format B) — lowercase all keys
            yield {k.lower(): v for k, v in item.items()}
        else:
            # Array of values (format A / C)
            yield dict(zip([c.lower() for c in columns], item))


# ── JSONL reader ──────────────────────────────────────────────────────────────

def _stream_jsonl(path: Path) -> Iterator[dict]:
    """Stream JSONL one line at a time — no full-file load into memory."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                log.warning("JSONL parse error %s line %d: %s", path.name, line_no, exc)


# ── CSV reader ────────────────────────────────────────────────────────────────

def _stream_csv(path: Path, encoding: str = "utf-8") -> Iterator[dict]:
    with open(path, "r", encoding=encoding, newline="", errors="replace") as f:
        # Sniff delimiter
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel  # default
        reader = csv.DictReader(f, dialect=dialect)
        yield from reader


# ── Excel reader ──────────────────────────────────────────────────────────────

def _stream_excel(path: Path) -> Iterator[dict]:
    try:
        import openpyxl
    except ImportError as exc:
        raise ImportError("openpyxl required for Excel support: pip install openpyxl") from exc

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    headers = [str(h).lower() if h is not None else f"col_{i}"
               for i, h in enumerate(next(rows))]
    for row in rows:
        yield dict(zip(headers, row))
    wb.close()


# ── Public API ────────────────────────────────────────────────────────────────

def stream_records(path: Path) -> Iterator[dict]:
    """
    Detect format and stream records one dict at a time.
    Works for all INSAF source files.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    fmt = detect_format(path)
    log.debug("Detected format '%s' for %s", fmt, path.name)

    if fmt == "json_oracle":
        yield from _stream_json_oracle(path)
    elif fmt == "jsonl":
        yield from _stream_jsonl(path)
    elif fmt in ("json_array", "json_object"):
        # Small flat JSON files
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        if isinstance(data, list):
            yield from data
        else:
            yield data
    elif fmt == "csv":
        yield from _stream_csv(path)
    elif fmt == "excel":
        yield from _stream_excel(path)
    else:
        raise ValueError(f"Unsupported format '{fmt}' for {path}")
