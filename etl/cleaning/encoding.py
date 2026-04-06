"""
Arabic encoding repair for INSAF source data.

ROOT CAUSE
----------
Oracle exported Arabic text encoded as CP1256 (Windows Arabic).
The export tool read those bytes as Latin-1 (ISO-8859-1), so each
CP1256 byte was interpreted as a Latin character and the result was
saved to the JSON file in UTF-8.

EXAMPLE
-------
  Stored in file:  ÚÇãá ãÊÑÓã
  Should be:       عامل مترسم  (Permanent Worker)

  Mechanism:
    Original byte 0xDA (CP1256 = ع) → Latin-1 reads it as Ú → saved as UTF-8 C3 9A
    To undo: encode the Ú back to Latin-1 bytes (→ 0xDA), then decode as CP1256 (→ ع)

FIX
---
  corrupted_str.encode('latin-1').decode('cp1256')

This is exact and deterministic — no heuristics, covers 100 % of the Arabic
character space.

DETECTION
---------
We only apply the fix when we are confident the string is mojibake.
Signature: high density of Latin-Extended characters (U+00C0–U+00FF)
combined with *no* real Arabic Unicode (U+0600–U+06FF).
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

# ── Known Arabic label fields across all INSAF files ─────────────────────────
ARABIC_FIELDS: frozenset[str] = frozenset({
    # grade.json
    "libcgrda", "liblgrda",
    # organisme.json
    "liborga",
    # region.json
    "lib_rega",
    # nature.json
    "libnata",
    # indem_def.json
    "tmi_liba",
    # clean dimension outputs (if re-processed)
    "grade_label_ar", "nature_label_ar", "liborga_ar",
    "lib_rega_ar", "indemnite_label_ar",
})

_JUNK_CHARS_RE = re.compile(
    r"[\ufeff\u200b\u200c\u200d\u200e\u200f"
    r"\u202a\u202b\u202c\u202d\u202e\x00]"
)


# ── Detection ─────────────────────────────────────────────────────────────────

def is_mojibake(text: str) -> bool:
    """
    Return True if *text* looks like Arabic CP1256 data read as Latin-1.

    Checks:
      1. No real Arabic Unicode present (U+0600–U+06FF).
      2. > 25 % of characters are in the Latin Extended range (U+00C0–U+00FF),
         which is the classic signature of this specific mojibake type.
    """
    if not text or not isinstance(text, str):
        return False
    # If there is already real Arabic Unicode → already correct, do NOT touch
    if any("\u0600" <= ch <= "\u06FF" for ch in text):
        return False
    alpha = [ch for ch in text if ch.isalpha()]
    if not alpha:
        return False
    latin_ext = sum(1 for ch in alpha if "\xc0" <= ch <= "\xff")
    return (latin_ext / len(alpha)) > 0.25


# ── Fix ───────────────────────────────────────────────────────────────────────

def fix_mojibake(text: str) -> str:
    """
    Reverse the Latin-1 mis-interpretation of CP1256 bytes.

    Step 1: encode the corrupted string back to Latin-1 bytes
            (recovers the original CP1256 byte sequence).
    Step 2: decode those bytes as CP1256 (Windows Arabic) → proper Unicode.

    If the encode/decode fails for any reason, the original text is returned
    unchanged so we never corrupt a value that was already correct.
    """
    try:
        return text.encode("latin-1").decode("cp1256")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def clean_string(text: str, field_name: str = "") -> str:
    """
    Full string normalization pipeline:
      1. Mojibake repair (if field is a known Arabic label or auto-detected)
      2. Remove invisible / junk characters (BOM, zero-width, direction marks)
      3. Unicode NFC normalization
      4. Strip leading/trailing whitespace
    """
    if not isinstance(text, str):
        return text

    # Fix mojibake — either field is a known Arabic label, or auto-detected
    if field_name.lower() in ARABIC_FIELDS or is_mojibake(text):
        text = fix_mojibake(text)

    # Remove junk chars
    text = _JUNK_CHARS_RE.sub("", text)

    # NFC normalization
    text = unicodedata.normalize("NFC", text)

    return text.strip()


# ── Record-level helper ───────────────────────────────────────────────────────

def fix_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Apply encoding fix to every string field in a record dict.
    Arabic label fields are fixed unconditionally; other string fields
    are fixed only if auto-detected as mojibake.
    Nested dicts and lists are handled recursively.
    Non-string values (int, float, bool, None) pass through unchanged.
    """
    out: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, str):
            out[key] = clean_string(value, field_name=key)
        elif isinstance(value, dict):
            out[key] = fix_record(value)
        elif isinstance(value, list):
            out[key] = [
                fix_record(v) if isinstance(v, dict)
                else clean_string(v, field_name=key) if isinstance(v, str)
                else v
                for v in value
            ]
        else:
            out[key] = value
    return out
