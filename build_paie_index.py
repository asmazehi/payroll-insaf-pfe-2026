"""
build_paie_index.py
===================
One-time scan of paie.json that records the approximate byte offset where
each year's data starts inside the items array.

Run once:
    python build_paie_index.py

Creates: data/newRawData/paie_year_index.json
Future ETL runs will use this index to seek directly to the target year
instead of scanning through the entire 54 GB file.

Expected runtime: 30-60 minutes (same as one full scan).
After that, year-filtered ETL runs go from ~45 min → ~2 min.
"""
import json
import sys
import time
from pathlib import Path

SOURCE     = Path("data/newRawData/paie.json")
INDEX_FILE = Path("data/newRawData/paie_year_index.json")
IJSON_CHUNK = 65536   # ijson default read buffer — our offset estimate is off by at most this much


class _ByteTracker:
    """Wraps a binary file and tracks how many bytes ijson has consumed."""
    def __init__(self, path: Path):
        self._f = open(path, "rb")
        self.pos = 0

    def read(self, n=-1):
        data = self._f.read(n)
        self.pos += len(data)
        return data

    def close(self):
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def _find_items_start(path: Path) -> int:
    """Return the byte offset of the '[' that opens the items array."""
    with open(path, "rb") as f:
        # items array is always within the first 200 KB (column definitions come first)
        head = f.read(200 * 1024)
    for pattern in (b'"items":[', b'"items": ['):
        idx = head.find(pattern)
        if idx != -1:
            bracket = head.index(b"[", idx + len(pattern) - 1)
            return bracket
    raise ValueError("Could not find 'items' array in file header")


def build():
    if not SOURCE.exists():
        print(f"ERROR: {SOURCE} not found.", file=sys.stderr)
        sys.exit(1)

    size_gb = SOURCE.stat().st_size / 1e9
    print(f"Scanning {SOURCE}  ({size_gb:.1f} GB)  — this takes 30-60 min, run once.")
    print("Progress is logged every 1 M records.\n")

    import ijson

    # ── Step 1: get column names and find pa_annee index ──────────────────────
    columns: list[str] = []
    has_results = False
    with open(SOURCE, "rb") as f:
        head = f.read(2048).decode("utf-8", errors="replace")
        has_results = '"results"' in head[:500]

    col_prefix   = "results.item.columns.item" if has_results else "columns.item"
    items_prefix = "results.item.items.item"   if has_results else "items.item"

    with open(SOURCE, "rb") as f:
        for prefix, event, value in ijson.parse(f, use_float=True):
            if prefix == col_prefix and isinstance(value, str):
                columns.append(value.lower())
            if prefix == col_prefix + ".name" and isinstance(value, str):
                columns.append(value.lower())
            if "items" in prefix and columns:
                break

    if not columns:
        print("ERROR: could not find column definitions.", file=sys.stderr)
        sys.exit(1)

    try:
        annee_idx = columns.index("pa_annee")
    except ValueError:
        print(f"ERROR: 'pa_annee' not found in columns: {columns}", file=sys.stderr)
        sys.exit(1)

    items_start_byte = _find_items_start(SOURCE)
    print(f"Columns: {len(columns)}  |  pa_annee at index {annee_idx}")
    print(f"Items array starts at byte {items_start_byte:,}  ({items_start_byte/1e9:.3f} GB)\n")

    # ── Step 2: stream all items, record year transitions ─────────────────────
    year_offsets: dict[str, int] = {}
    current_year: int | None = None
    count = 0
    t0 = time.time()

    with _ByteTracker(SOURCE) as tracker:
        for item in ijson.items(tracker, items_prefix, use_float=True):
            if isinstance(item, list):
                raw_yr = item[annee_idx] if len(item) > annee_idx else 0
            else:
                raw_yr = item.get("pa_annee") or item.get("PA_ANNEE") or 0

            try:
                yr = int(float(raw_yr)) if raw_yr else 0
            except (ValueError, TypeError):
                yr = 0

            if yr and yr != current_year:
                # Subtract IJSON_CHUNK because ijson may have buffered ahead
                approx_offset = max(items_start_byte + 1, tracker.pos - IJSON_CHUNK)
                year_offsets[str(yr)] = approx_offset
                elapsed = time.time() - t0
                print(f"  Year {yr:4d}  |  ~byte {approx_offset:>13,}  ({approx_offset/1e9:5.2f} GB)"
                      f"  |  {count:>9,} records  |  {elapsed/60:.1f} min elapsed")
                current_year = yr

            count += 1
            if count % 1_000_000 == 0:
                pct = tracker.pos / SOURCE.stat().st_size * 100
                elapsed = time.time() - t0
                eta = (elapsed / pct * (100 - pct)) if pct > 0 else 0
                print(f"  ... {count:,} records  |  {pct:.1f}% read  |  ETA {eta/60:.0f} min")

    # ── Step 3: save index ────────────────────────────────────────────────────
    index = {
        "_meta": {
            "source":           str(SOURCE),
            "source_size_bytes": SOURCE.stat().st_size,
            "items_start_byte": items_start_byte,
            "columns":          columns,
            "annee_col_index":  annee_idx,
            "has_results_wrap": has_results,
            "items_prefix":     items_prefix,
            "col_prefix":       col_prefix,
            "total_records":    count,
            "built_at":         time.strftime("%Y-%m-%dT%H:%M:%S"),
        },
        "year_offsets": year_offsets,
    }

    INDEX_FILE.write_text(json.dumps(index, indent=2))
    print(f"\n✓  Index saved to {INDEX_FILE}")
    print(f"   Years indexed: {sorted(int(y) for y in year_offsets)}")
    print(f"   Total records: {count:,}  in  {(time.time()-t0)/60:.1f} min")
    print("\nFuture ETL runs with --year-min will skip directly to the target year.")


if __name__ == "__main__":
    build()
