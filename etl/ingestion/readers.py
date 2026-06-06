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

# Files larger than this use ijson streaming instead of full in-memory load
_LARGE_FILE_THRESHOLD = 50 * 1024 * 1024  # 50 MB

# ── Year-seek index ───────────────────────────────────────────────────────────

def _load_year_index(source: Path) -> dict | None:
    """Load paie_year_index.json if it exists next to the source file."""
    idx_path = source.parent / (source.stem + "_year_index.json")
    if not idx_path.exists():
        return None
    try:
        return json.loads(idx_path.read_text())
    except Exception:
        return None


class _YearSeekReader:
    """
    Presents paie.json to ijson as if it starts from a given year.

    Splices the original JSON header (columns + items array opening bracket)
    with the file content starting from `seek_byte`, so ijson sees a
    syntactically complete Oracle JSON file containing only the data from
    approximately `year_min` onwards.
    """

    def __init__(self, path: Path, items_start_byte: int, seek_byte: int):
        # Read header: everything up to and including the '[' that opens items
        with open(path, "rb") as f:
            self._header = f.read(items_start_byte + 1)   # +1 includes the '[' itself

        # Align to an item boundary: scan a 64 KB window around seek_byte
        # looking for '],[' (array-of-arrays) or '},{' (dict items).
        # Rows are typically 5-10 KB so 64 KB guarantees we'll find a boundary.
        WINDOW = 65536
        with open(path, "rb") as f:
            scan_start = max(items_start_byte + 2, seek_byte - WINDOW // 2)
            f.seek(scan_start)
            window = f.read(WINDOW)

        boundary = self._find_item_boundary(window)
        if boundary != -1:
            aligned = scan_start + boundary
        else:
            aligned = seek_byte

        self._f = open(path, "rb")
        self._f.seek(aligned)

        self._header_pos = 0
        self._serving_header = True

        log.info("YearSeekReader: items_start=%d  seek=%d  aligned=%d  (skipped %.2f GB)",
                 items_start_byte, seek_byte, aligned, aligned / 1e9)

    @staticmethod
    def _find_item_boundary(window: bytes) -> int:
        """Return the position of the '{' or '[' that starts the next item.
        Tries all known Oracle JSON separator patterns, most specific first."""
        for sep, offset in [
            (b"}\r\n,{", 4),   # dict items with CRLF  → land on {
            (b"}\n,{",  3),    # dict items with LF    → land on {
            (b"},{",    2),    # dict items no newline → land on {
            (b"]\r\n,[", 4),   # array items with CRLF → land on [
            (b"]\n,[",  3),    # array items with LF  → land on [
            (b"],[",    2),    # array items no newline→ land on [
        ]:
            pos = window.rfind(sep)
            if pos != -1:
                return pos + offset   # position of opening { or [
        return -1

    def read(self, n: int = -1) -> bytes:
        if self._serving_header:
            chunk = self._header[self._header_pos: self._header_pos + n]
            self._header_pos += len(chunk)
            remaining = n - len(chunk)
            if self._header_pos >= len(self._header):
                self._serving_header = False
                if remaining > 0:
                    chunk += self._f.read(remaining)
            return chunk
        return self._f.read(n)

    def close(self) -> None:
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class _FixedDecimalReader(io.RawIOBase):
    """
    Wraps a binary file and fixes French decimal commas on the fly.
    e.g.  "pa_indice":0,04  →  "pa_indice":0.04
    Allows ijson to stream files that would otherwise fail json.loads.

    Chunk-boundary correctness: we apply the regex to ALL accumulated raw
    bytes each fill, emit all but the last _MARGIN bytes, then put those
    _MARGIN raw (unprocessed) bytes back into _raw for the next fill.
    Because the regex is idempotent (replacing , → . never re-matches),
    overlapping the margin across fills is safe and guarantees every match
    is seen whole regardless of where the 64 KB chunk boundary falls.
    """
    _CHUNK  = 1 << 16  # 64 KB read buffer
    _MARGIN = 50       # > max possible match length (":\s*-?\d+,\d" ≈ 25 bytes)
    _PAT    = re.compile(rb'(:\s*-?\d+),(\d)')

    def __init__(self, path: Path):
        self._fp  = open(path, "rb")
        self._out = bytearray()   # fixed bytes ready to emit
        self._raw = bytearray()   # raw bytes pending regex (includes overlap)
        self._eof = False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def readable(self):
        return True

    def _fill(self):
        chunk = self._fp.read(self._CHUNK)
        if not chunk:
            self._eof = True
            self._out.extend(self._PAT.sub(rb'\1.\2', bytes(self._raw)))
            self._raw.clear()
            return
        self._raw.extend(chunk)
        if len(self._raw) > self._MARGIN:
            # Apply regex to everything we have — catches cross-boundary matches
            fixed = self._PAT.sub(rb'\1.\2', bytes(self._raw))
            # Emit all but the last _MARGIN bytes of the FIXED output
            emit = len(fixed) - self._MARGIN   # same length since , and . are 1 byte each
            self._out.extend(fixed[:emit])
            # Keep the last _MARGIN bytes of the FIXED output as the new overlap.
            # Using fixed (not self._raw) is critical: if the comma was right at the
            # emit boundary, the replacement dot must carry over into the next fill,
            # not the original comma from self._raw.
            self._raw = bytearray(fixed[-self._MARGIN:])

    def readinto(self, b):
        while len(self._out) < len(b) and not self._eof:
            self._fill()
        n = min(len(b), len(self._out))
        if n == 0:
            return 0
        b[:n] = self._out[:n]
        del self._out[:n]
        return n

    def close(self):
        if not self._fp.closed:
            self._fp.close()
        super().close()


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


# ── Arabic encoding repair ───────────────────────────────────────────────────
# Oracle exports from AR8MSWIN1256 (cp1256) databases dump raw Arabic bytes
# into JSON that is labeled UTF-8. Each cp1256 byte gets misread as Latin-1
# and stored as a 2-byte UTF-8 sequence (U+00C0–U+00FF range).
# Fix: re-encode as Latin-1 to recover the original cp1256 bytes, then decode.

def fix_arabic_mojibake(s: str) -> str:
    if not s:
        return s
    try:
        return s.encode('latin-1').decode('cp1256')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def fix_record_arabic(record: dict) -> dict:
    """Apply Arabic mojibake fix to all string values in a record."""
    return {
        k: fix_arabic_mojibake(v) if isinstance(v, str) else v
        for k, v in record.items()
    }


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

def _stream_json_oracle(path: Path, **kwargs) -> Iterator[dict]:
    """
    Handle Oracle export flavours:
      A)  {"columns":[...], "items":[[v,v,...], ...]}    — array-of-arrays
      B)  {"results":[{"columns":[...], "items":[{...},...]}]}  — results wrapper

    Small files (<50 MB): full in-memory parse (fast).
    Large files: ijson streaming — constant memory regardless of file size.
    French comma decimals repaired in both paths.
    """
    log.debug("Reading Oracle JSON: %s (%.1f MB)",
              path.name, path.stat().st_size / 1e6)

    if path.stat().st_size < _LARGE_FILE_THRESHOLD:
        # ── Small file: full in-memory load ──────────────────────────────────
        raw = path.read_bytes()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("latin-1")
        text = _repair_comma_decimals(text)
        data = json.loads(text)
        block = data["results"][0] if "results" in data else data
        columns = [
            (col["name"] if isinstance(col, dict) else col).lower()
            for col in block["columns"]
        ]
        for item in block["items"]:
            if isinstance(item, dict):
                rec = {k.lower(): v for k, v in item.items()}
            else:
                rec = dict(zip(columns, item))
            yield fix_record_arabic(rec)
        return

    # ── Large file: ijson streaming ───────────────────────────────────────────
    import ijson

    # Peek to detect "results" wrapper
    with open(path, "rb") as f:
        head = f.read(2048).decode("utf-8", errors="replace").lstrip()
    has_results = '"results"' in head[:500]

    col_prefix   = "results.item.columns.item" if has_results else "columns.item"
    items_prefix = "results.item.items.item"    if has_results else "items.item"

    # Pass 1 — extract column names (fast: stops once "items" key is reached)
    columns: list[str] = []
    with _FixedDecimalReader(path) as f:
        for prefix, event, value in ijson.parse(f, use_float=True):
            if prefix == col_prefix and isinstance(value, str):
                columns.append(value.lower())
            # dict-format columns: {"name": "PA_ANNEE"}
            if prefix == col_prefix + ".name" and isinstance(value, str):
                columns.append(value.lower())
            if "items" in prefix and columns:
                break

    if not columns:
        raise ValueError(f"No columns found in Oracle JSON: {path.name}")

    log.info("Oracle JSON columns (%d): %s…", len(columns), columns[:5])

    # ── Year-seek optimisation ────────────────────────────────────────────────
    year_min = kwargs.get("year_min")
    seek_reader = None
    if year_min is not None:
        idx = _load_year_index(path)
        if idx:
            offsets = idx.get("year_offsets", {})
            items_start = idx["_meta"]["items_start_byte"]
            seek_byte = offsets.get(str(year_min))
            if seek_byte:
                log.info("Year index hit: seeking to ~%.2f GB for year %d",
                         seek_byte / 1e9, year_min)
                seek_reader = _YearSeekReader(path, items_start, seek_byte)
            else:
                log.info("Year %d not in index — full scan", year_min)

    # Pass 2 — stream items one at a time
    reader = seek_reader if seek_reader else _FixedDecimalReader(path)
    with reader as f:
        for item in ijson.items(f, items_prefix, use_float=True):
            if isinstance(item, dict):
                rec = {k.lower(): v for k, v in item.items()}
            elif isinstance(item, list):
                rec = dict(zip(columns, item))
            else:
                continue
            yield fix_record_arabic(rec)


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

def stream_records(path: Path, **kwargs) -> Iterator[dict]:
    """
    Detect format and stream records one dict at a time.
    Works for all INSAF source files.
    Pass year_min=<int> to use the byte-offset index for Oracle JSON files.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    fmt = detect_format(path)
    log.debug("Detected format '%s' for %s", fmt, path.name)

    if fmt == "json_oracle":
        yield from _stream_json_oracle(path, **kwargs)
    elif fmt == "jsonl":
        yield from _stream_jsonl(path)
    elif fmt in ("json_array", "json_object"):
        if fmt == "json_array" and path.stat().st_size >= _LARGE_FILE_THRESHOLD:
            import ijson
            with _FixedDecimalReader(path) as f:
                for item in ijson.items(f, "item", use_float=True):
                    yield item
        else:
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
